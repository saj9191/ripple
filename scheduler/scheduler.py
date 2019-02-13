import argparse
import boto3
import inspect
import json
import os
import priority_queue
import sys
import threading
import time
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import database
import setup
import util


def payload(bucket, key):
  return {
   "Records": [{
     "s3": {
       "bucket": {
         "name": bucket,
       },
       "object": {
         "key": key,
       },
     }
    }]
  }


class Task(threading.Thread):
  def __init__(self, bucket_name, key, timeout, params):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.check_time = time.time()
    self.key = key
    self.params = params
    self.payloads = {key: []}
    self.processed = set()
    self.running = True
    self.stage = 1
    self.timeout = timeout
    self.token = key.split("/")[1]
    self.client = util.setup_client("lambda", self.params)

  def __get_children_payloads__(self, s3, object_key):
    obj = s3.Object(self.params["log"], object_key)
    content = obj.get()["Body"].read()
    params = json.loads(content.decode("utf-8"))
    return params["payloads"]

  def __expected_num_objects__(self, objs):
    if len(objs) == 0:
      expected_num_objs = 1
    else:
      m = util.parse_file_name(objs[0])
      expected_num_objs = m["num_bins"] * m["num_files"]
    return expected_num_objs

  def __get_output_key__(self, prefix, payload):
    s3_payload = payload["Records"][0]["s3"]
    object_key = s3_payload["object"]["key"]
    input_format = util.parse_file_name(object_key)
    params = {**self.params, **s3_payload}
    if "extra_params" in s3_payload:
      params = {**params, **s3_payload["extra_params"]}

    params["prefix"] = prefix
    name = self.params["pipeline"][prefix]["name"]
    params["file"] = self.params["functions"][name]["file"]
    [output_format, bucket_format] = util.get_formats(input_format, params)
    return util.file_name(bucket_format)

  def __invoke__(self, name, payload):
    self.params["s3"].invoke(self.client, name, self.params, payload)

  def __process_object__(self, s3, obj):
    if obj not in self.processed:
      for payload in self.__get_children_payloads__(s3, obj):
        self.payloads[self.__get_output_key__(self.stage, payload)] = payload
      self.processed.add(obj)

  def run(self):
    print("Processing", self.key)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(self.bucket_name)

    while self.stage <= len(self.params["pipeline"]):
      objs = list(map(lambda o: o.key, bucket.objects.filter(Prefix=str(self.stage) + "/" + self.token)))
      expected_num_objs = self.__expected_num_objects__(objs)

      if self.stage < len(self.params["pipeline"]):
        for obj in objs:
          self.__process_object__(s3, obj)

      if len(objs) == expected_num_objs:
        for obj in objs:
          del self.payloads[obj]
        self.stage += 1
        self.check_time = time.time()
      else:
        if time.time() - self.check_time > self.timeout:
          found = {}
          for obj in objs:
            m = util.parse_file_name(obj)
            if m["bin"] not in found:
              found[m["bin"]] = set()
            found[m["bin"]].add(m["file_id"])
          for bin_id in range(1, m["num_bins"] + 1):
            for file_id in range(1, m["num_files"] + 1):
              if bin_id not in found or file_id not in found[bin_id]:
                m["bin"] = bin_id
                m["file_id"] = file_id
                name = self.params["pipeline"][m["prefix"] - 1]["name"]
                key = util.file_name(m)
                print("Cannot find", key, "Reinvoking", name)
                print(self.payloads[key])
                self.__invoke__(name, key)
          self.check_time = time.time()

    print("Done processing", self.key)
    self.running = False


class Scheduler:
  def __init__(self, policy, timeout, params):
    self.max_tasks = 1000
    self.next_job_id = 0
    self.params = params
    self.policy = policy
    self.running = True
    self.running_tasks = {}
    self.__setup__()
    self.tasks = []
    self.timeout = timeout

  def __setup__(self):
    if self.policy == "fifo":
      self.queue = priority_queue.Fifo()
    elif self.policy == "robin":
      self.queue = priority_queue.Robin()
    elif self.policy == "deadline":
      self.queue = priority_queue.Deadline()
    else:
      raise Exception("Unknown scheduling policy", self.policy)

    self.__setup_sqs_queues__()
    self.__aws_connections__()

  def __setup_sqs_queue__(self, bucket_name, filter_prefix=None):
    client = boto3.client("sqs", region_name=self.params["region"])
    name = "sqs-" + bucket_name
    response = client.list_queues(QueueNamePrefix=name)
    urls = response["QueueUrls"] if "QueueUrls" in response else []
    assert(len(urls) <= 1)
    sqs = boto3.resource("sqs")
    if len(urls) == 0:
      queue = sqs.create_queue(QueueName=name, Attributes={"DelaySeconds": "5"})
    else:
      queue = sqs.get_queue_by_name(QueueName=name)
    # Remove stale SQS messages
    client.purge_queue(QueueUrl=queue.url)

    policy = {
      "Statement": [{
        "Effect": "Allow",
        "Principal": {
          "AWS": "*",
        },
        "Action": [
            "SQS:SendMessage"
        ],
        "Resource": queue.attributes["QueueArn"],
      }]
    }

    client.set_queue_attributes(QueueUrl=queue.url, Attributes={"Policy": json.dumps(policy)})
    client = boto3.client("s3", region_name=self.params["region"])
    configuration = client.get_bucket_notification_configuration(Bucket=bucket_name)
    del configuration["ResponseMetadata"]
    configuration["QueueConfigurations"] = [{
      "Events": ["s3:ObjectCreated:*"],
      "Id": "Notifications",
      "QueueArn": queue.attributes["QueueArn"]
    }]
    if filter_prefix is not None:
      configuration["QueueConfigurations"][0]["Filter"] = {
        "Key": {
          "FilterRules": [{
            "Name": "Prefix",
            "Value": filter_prefix,
          }]
        }
      }

    client.put_bucket_notification_configuration(
      Bucket=bucket_name,
      NotificationConfiguration=configuration
    )
    return queue

  def __setup_sqs_queues__(self):
    self.log_queue = self.__setup_sqs_queue__(self.params["log"], filter_prefix="1/")
#   self.bucket_queue = self.__setup_sqs_queue__(self.params["bucket"], filter_prefix="0/")

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")
    #self.client = util.setup_client("lambda", self.params)

  def add(self, priority, deadline, payload, prefix=0):
    item = priority_queue.Item(priority, prefix, self.next_job_id, deadline, payload, self.params)
    self.next_job_id += 1
    self.queue.put(item)

  def __object_exists__(self, object_key):
    return util.object_exists(self.s3, self.params["log"], object_key)

  def __get_messages__(self, queue):
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      AttributeNames=["SentTimestamp"],
      MaxNumberOfMessages=10,
      QueueUrl=queue.url,
      WaitTimeSeconds=1,
    )
    messages = response["Messages"] if "Messages" in response else []
    return messages

  def __check_tasks__(self):
    messages = self.__get_messages__(self.log_queue)
    sqs = boto3.client("sqs", region_name=self.params["region"])
    for message in messages:
      body = json.loads(message["Body"])
      if "Records" in body:
        for record in body["Records"]:
          key = record["s3"]["object"]["key"]
          self.tasks.append(Task(self.params["log"], key, self.timeout, self.params))
          self.tasks[-1].start()
      sqs.delete_message(QueueUrl=self.log_queue.url, ReceiptHandle=message["ReceiptHandle"])

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running:
        self.tasks[i].join()
        self.tasks.pop(i)
      else:
        i += 1

  def __running__(self):
    return self.running

  def listen(self):
    while self.__running__():
      self.__check_tasks__()


def run(policy, timeout, params):
  scheduler = Scheduler(policy, timeout, params)
  scheduler.listen()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--policy", type=str, default="fifo", help="Scheduling policy to use (fifo, robin, deadline)")
  parser.add_argument("--timeout", type=int, default=60, help="How long we should wait for a task to retrigger")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup.process_functions(params)
  params["s3"] = database.S3()
  run(args.policy, args.timeout, params)


if __name__ == "__main__":
  main()

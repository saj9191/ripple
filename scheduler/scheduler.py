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
  def __init__(self, bucket_name, token, timeout, params):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.key_to_payload = {}
    self.check_time = time.time()
    self.found = {}
    self.params = params
    self.processed = set()
    self.running = True
    self.stage = 0
    self.timeout = timeout
    self.token = token
    self.__setup_client__()

  def __current_logs__(self, objs):
    logs = set()
    max_bin = None
    max_file = None
    m = {}
    for obj in objs:
      m = util.parse_file_name(obj.key)
      logs.add((m["prefix"], m["bin"], m["file_id"]))
      max_bin = m["num_bins"]
      max_file = m["num_files"]
    mx = m["num_bins"] * m["num_files"] if max_bin and max_file else None
    return [logs, max_bin, max_file, mx]

  def __get_children_payloads__(self, stage, objs):
    if stage == -1:
      return set([(0, 1, 1)])

    logs = set()
    for obj in objs:
      key = obj.key
      if key not in self.processed:
        self.processed.add(key)
        body = self.__get_object__(key)
        for p in body["payloads"]:
          key = self.__get_key__(p)
          logs.add(key)
          self.key_to_payload[key] = p
    return logs

  def __get_key__(self, payload):
    m = util.parse_file_name(payload["Records"][0]["s3"]["object"]["key"])
    if "extra_params" in payload["Records"][0]["s3"]:
      m = {**m, **payload["Records"][0]["s3"]["extra_params"]}
    return (m["prefix"], m["bin"], m["file_id"])

  def __get_object__(self, key):
    obj = self.s3.Object(self.params["log"], key)
    content = obj.get()["Body"].read().decode("utf-8")
    return json.loads(content)

  def __get_objects__(self, stage):
    objs = self.bucket.objects.filter(Prefix=str(stage) + "/" + self.token)
    return objs

  def __invoke__(self, name, payload):
    response = self.client.invoke(
      FunctionName=name,
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  def __running__(self):
    return True

  def __setup_client__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)
    self.client = util.setup_client("lambda", self.params)
    self.log = self.s3.Bucket(self.params["log"])

  def __find_payloads__(self, actual_logs, max_bin, max_file):
    missing = set()
    parent_logs = self.found[self.stage - 1]
    parent_obj = list(self.__get_objects__(self.stage - 1))[0]
    m = util.parse_file_name(parent_obj.key)
    if len(actual_logs) > 0:
      sibling_objs = list(self.__get_objects__(self.stage))
      m = {**m, **util.parse_file_name(sibling_objs[0].key)}

    max_parent_bin = max(list(map(lambda l: l[1], parent_logs)))
    max_parent_file = max(list(map(lambda l: l[2], parent_logs)))

    if max_bin is None:
      # If we have 0 files, we're going to assume for now the num bins / files
      # is the same as the parents
      max_bin = max_parent_bin
      max_file = max_parent_file

    for bin_id in range(1, max_bin + 1):
      for file_id in range(1, max_file + 1):
        ll = (self.stage, bin_id, file_id)
        if ll not in actual_logs:
          missing.add(ll)

    payloads = []
    m["timestamp"] = float(self.token.split("-")[0])
    m["nonce"] = int(self.token.split("-")[1])
    for (s, b, f) in missing:
      print("Cannot find", (s, b, f))
      if (s-1, b, f) in parent_logs:
        # First case. The parent only has one child and the same bin and file id
        m["prefix"] = self.stage - 1
        m["bin"] = b
        m["num_bins"] = max_parent_bin
        m["file_id"] = f
        m["num_files"] = max_parent_file
        body = self.__get_object__(util.file_name(m))
        payloads += body["payloads"]
      else:
        assert((s-1, 1, 1) in parent_logs)
        assert(max_parent_bin == 1)
        assert(max_parent_file == 1)
        # Third case. The child is the result of some split variation. So the max file ID
        # of the parent must be 1.
        m["prefix"] = self.stage - 1
        m["bin"] = 1
        m["num_bins"] = 1
        m["file_id"] = 1
        m["num_files"] = 1
        body = self.__get_object__(util.file_name(m))
        payloads += body["payloads"]
      return payloads

  def run(self):
    print(self.token, "Starting stage", self.stage)
    while self.__running__() and self.stage < len(self.params["pipeline"]):
      [actual_logs, max_bin, max_file, expected_num_bins] = self.__current_logs__(self.__get_objects__(self.stage))
      self.found[self.stage] = actual_logs
      if len(actual_logs) == expected_num_bins:
        # We have all the payloads for this stage
        self.stage += 1
        print(self.token, "Starting stage", self.stage)
      else:
        if time.time() - self.check_time > self.timeout:
          payloads = self.__find_payloads__(actual_logs, max_bin, max_file)
          for payload in payloads:
            name = self.params["pipeline"][self.stage]["name"]
            print(self.token, "Cannot find", payload, "Reinvoking", name)
            self.__invoke__(name, payload)
          self.check_time = time.time()
      time.sleep(5)

    print("Done processing", self.token)
    self.running = (self.stage < len(self.params["pipeline"]))


class Scheduler:
  def __init__(self, policy, timeout, params):
    self.max_tasks = 1000
    self.messages = {}
    self.next_job_id = 0
    self.params = params
    self.policy = policy
    self.prefixes = set()
    self.running = True
    self.__setup__()
    self.tasks = []
    self.timeout = timeout
    self.tokens = set()

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")

  def __add_task__(self, token):
    self.tasks.append(Task(self.params["log"], token, self.timeout, self.params))

  def __check_tasks__(self):
    self.__get_messages__(self.log_queue)
    tokens = self.messages[0].keys() if 0 in self.messages else []
    for token in tokens:
      if token not in self.tokens:
        self.__add_task__(token)
        self.tasks[-1].start()
        self.tokens.add(token)

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running:
        self.tasks[i].join()
        self.tasks.pop(i)
      else:
        i += 1

  def __delete_message__(self, queue, message):
    self.sqs.delete_message(QueueUrl=self.log_queue.url, ReceiptHandle=message["ReceiptHandle"])

  def __get_messages__(self, queue):
    messages = self.__fetch_messages__(self.log_queue)
    for message in messages:
      body = json.loads(message["Body"])
      if "Records" in body:
        for record in body["Records"]:
          key = record["s3"]["object"]["key"]
          parts = key.split("/")
          prefix = int(parts[0])
          token = parts[1]
          self.prefixes.add(prefix)
          if prefix not in self.messages:
            self.messages[prefix] = {}
          if token not in self.messages[prefix]:
            self.messages[prefix][token] = []
          assert(prefix in self.messages)
          assert(prefix in self.prefixes)
          self.messages[prefix][token].append(body)
      self.__delete_message__(self.log_queue, message)

  def __fetch_messages__(self, queue):
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      AttributeNames=["SentTimestamp"],
      MaxNumberOfMessages=10,
      QueueUrl=queue.url,
      WaitTimeSeconds=1,
    )
    messages = response["Messages"] if "Messages" in response else []
    return messages

  def __running__(self):
    return self.running

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
    urls = list(map(lambda url: url == name, urls))
    sqs = boto3.resource("sqs")
    if len(urls) == 0:
      print("Creating queue", name, "in", self.params["region"])
      response = sqs.create_queue(QueueName=name, Attributes={"DelaySeconds": "5"})
      print(response)
      queue = sqs.get_queue_by_name(QueueName=name)
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
    self.log_queue = self.__setup_sqs_queue__(self.params["log"], "0/")
    self.sqs = boto3.client("sqs", region_name=self.params["region"])

  def add(self, priority, deadline, payload, prefix=0):
    item = priority_queue.Item(priority, prefix, self.next_job_id, deadline, payload, self.params)
    self.next_job_id += 1
    self.queue.put(item)

  def listen(self):
    print("Listening")
    while self.__running__():
      self.__check_tasks__()
    print("Done Listening")
    for task in self.tasks:
      task.join()


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

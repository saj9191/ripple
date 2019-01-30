import argparse
import boto3
import inspect
import json
import os
import priority_queue
import sys
import time
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
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


class Scheduler:
  def __init__(self, policy, params):
    self.max_tasks = 1000
    self.next_job_id = 0
    self.params = params
    self.policy = policy
    self.running = True
    self.running_tasks = {}
    self.__setup__()

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
    self.log_queue = self.__setup_sqs_queue__(self.params["log"])
#   self.bucket_queue = self.__setup_sqs_queue__(self.params["bucket"], filter_prefix="0/")

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")
    self.client = util.setup_client("lambda", self.params)

  def add(self, priority, deadline, payload, prefix=0):
    item = priority_queue.Item(priority, prefix, self.next_job_id, deadline, payload, self.params)
    self.next_job_id += 1
    self.queue.put(item)

  def __object_exists__(self, object_key):
    return util.object_exists(self.s3, self.params["log"], object_key)

  def __get_children_payloads__(self, object_key):
    obj = self.s3.Object(self.params["log"], object_key)
    content = obj.get()["Body"].read()
    params = json.loads(content.decode("utf-8"))
    return params["payloads"]

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
          m = util.parse_file_name(key)
          if m["prefix"] == len(self.params["pipeline"]):
            print("Finished", key)
          if key in self.running_tasks:
            del self.running_tasks[key]
          payloads = self.__get_children_payloads__(key)
          for payload in payloads:
            # TODO: Fix
            item = priority_queue.Item(1, m["prefix"], self.next_job_id, None, payload, self.params)
            self.running_tasks[item.output_file] = item
      sqs.delete_message(QueueUrl=self.log_queue.url, ReceiptHandle=message["ReceiptHandle"])

    keys = self.running_tasks.keys()
    for key in keys:
      task = self.running_tasks[key]
      if time.time() - task.start_time >= self.params["timeout"]:
        # Reinvoke
        print("Reinvoking", key)
        task.start_time = time.time()
        name = self.params["pipeline"][task.prefix]["name"]
        self.__invoke__(name, task.payload)

  def __invoke__(self, name, payload):
    util.invoke(self.client, name, self.params, payload)

  def __find_new_tasks__(self):
    messages = self.__get_messages__(self.bucket_queue)
    sqs = boto3.client("sqs", region_name=self.params["region"])
    for message in messages:
      body = json.loads(message["Body"])
      if "Records" in body:
        for record in body["Records"]:
          key = record["s3"]["object"]["key"]
          print("Starting new job", key)
          item = priority_queue.Item(1, 0, self.next_job_id, None, {"Records": [record]}, self.params)
          self.next_job_id += 1
          self.running_tasks[key] = item

      sqs.delete_message(QueueUrl=self.bucket_queue.url, ReceiptHandle=message["ReceiptHandle"])

  def __running__(self):
    return self.running

  def listen(self):
    while self.__running__():
      self.__check_tasks__()


def run(policy, params):
  scheduler = Scheduler(policy, params)
  scheduler.listen()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--policy", type=str, default="fifo", help="Scheduling policy to use (fifo, robin, deadline)")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup.process_functions(params)
  params["payloads"] = []
  run(args.policy, params)


if __name__ == "__main__":
  main()

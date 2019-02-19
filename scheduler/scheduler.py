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
  def __init__(self, bucket_name, token, messages, timeout, params):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.key_to_payload = {}
    self.check_time = time.time()
    self.messages = messages
    self.params = params
    self.processed = set()
    self.running = True
    self.stage = 0
    self.timeout = timeout
    self.token = token
    self.__setup_client__()

  def __current_logs__(self, payloads):
    logs = set()
    for payload in payloads:
      logs.add(self.__get_key__(payload))
    return logs

  def __get_children_payloads__(self, stage, payloads):
    if stage == -1:
      return set([(0, 1, 1)])

    if len(payloads) == 0:
      return set()

    logs = set()
    for payload in payloads:
      key = payload["Records"][0]["s3"]["object"]["key"]
      if key not in self.processed:
        self.processed.add(key)
        body = self.__get_object__(key)
        for p in body["payloads"]:
          key = self.__get_key__(p)
          logs.add(key)
          self.key_to_payload[key] = payload
    return logs

  def __get_key__(self, payload):
    m = util.parse_file_name(payload["Records"][0]["s3"]["object"]["key"])
    if "extra_params" in payload["Records"][0]["s3"]:
      m = {**m, **payload["Records"][0]["s3"]["extra_params"]}
    return (m["prefix"], m["bin"], m["file_id"])

  def __get_object__(self, key):
    return json.load(self.s3.Object(self.params["log"], key).get()["Body"].read().decode("utf-8"))

  def __get_payloads__(self, stage):
    if stage in self.messages:
      if self.token in self.messages[stage]:
        return self.messages[stage][self.token]
    return []

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

  def run(self):
    expected_logs = None
    print("Starting stage", self.stage)
    while self.__running__() and self.stage < len(self.params["pipeline"]):
      actual_logs = self.__current_logs__(self.__get_payloads__(self.stage))
      if expected_logs is None:
        expected_logs = self.__get_children_payloads__(self.stage - 1, self.__get_payloads__(self.stage - 1))

      if len(actual_logs) == len(expected_logs):
        # We have all the payloads for this stage
        self.stage += 1
        print("Starting stage", self.stage)
        expected_logs = None
      else:
        assert(len(actual_logs) < len(expected_logs))
        if time.time() - self.check_time > self.timeout:
          missing_logs = expected_logs.difference(actual_logs)
          for log in missing_logs:
            name = self.params["pipeline"][self.stage]["name"]
            print("Cannot find", log, "Reinvoking", name)
            self.__invoke__(name, self.key_to_payload[log])
          self.check_time = time.time()

    print("Done processing", self.token)
    self.running = (self.stage < len(self.params["pipeline"]))


class Scheduler:
  def __init__(self, policy, timeout, params):
    self.max_tasks = 1000
    self.messages = {0: {}}
    self.next_job_id = 0
    self.params = params
    self.policy = policy
    self.running = True
    self.__setup__()
    self.tasks = []
    self.timeout = timeout
    self.tokens = set()

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")

  def __add_task__(self, token):
    self.tasks.append(Task(self.params["log"], token, self.messages, self.timeout, self.params))

  def __check_tasks__(self):
    self.__get_messages__(self.log_queue)
    for token in self.messages[0].keys():
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
          if prefix not in self.messages:
            self.messages[prefix] = {}
          if token not in self.messages[prefix]:
            self.messages[prefix][token] = []
          self.messages[prefix][token].append(body)
      self.__delete_message__(self.log_queue, message)

  def __fetch_messages__(self, queue):
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      AttributeNames=["SentTimestamp"],
      MaxNumberOfMessages=100,
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
    self.log_queue = self.__setup_sqs_queue__(self.params["log"])
    self.sqs = boto3.client("sqs", region_name=self.params["region"])

  def add(self, priority, deadline, payload, prefix=0):
    item = priority_queue.Item(priority, prefix, self.next_job_id, deadline, payload, self.params)
    self.next_job_id += 1
    self.queue.put(item)

  def listen(self):
    while self.__running__():
      self.__check_tasks__()
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

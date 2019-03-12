import argparse
import boto3
import inspect
import json
import os
import queue
import random
import sys
import threading
import time
from typing import List, Tuple
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import database
import setup
import upload
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


class Job:
  def __init__(self, source_bucket, destination_bucket, key, start_time, deadline=None, priority=None, upload=False):
    self.deadline = deadline if deadline else sys.maxsize
    self.destination_bucket = destination_bucket
    self.key = key
    self.pause = [-1.0, -1.0]
    self.priority = priority if priority else 0
    self.source_bucket = source_bucket
    self.start_time = start_time
    self.upload = upload

  def __repr__(self):
    return "Job[start_time: {0:f}, deadline: {1:f}, priority: {2:d}, pause: ({3:f},{4:f})]".format(self.start_time, self.deadline, self.priority, self.pause[0], self.pause[1])


Order = Tuple[Job, float]


class Invoker(threading.Thread):
  def __init__(self, invoker_queue, thread_id, params):
    super(Invoker, self).__init__()
    self.invoker_queue = invoker_queue
    self.running = True
    self.thread_id = thread_id
    self.__setup_client__(params)

  def __setup_client__(self, params):
    s3 = boto3.resource("s3")
    self.client = boto3.client("lambda")

  def __invoke__(self, name, payload):
    response = self.client.invoke(
      FunctionName=name,
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  def __running__(self):
    return True

  def run(self):
    while self.__running__():
      name, payload = self.invoker_queue.get()
      self.__invoke__(name, payload)


class Logger(threading.Thread):
  def __init__(self, bucket_name, queue):
    super(Logger, self).__init__()
    self.bucket_name = bucket_name
    self.queue = queue
    self.__setup__()

  def __get_log__(self, name):
    obj = self.s3.Object(self.bucket_name, name)
    body = json.loads(obj.get()["Body"].read().decode("utf-8"))
    return body

  def __setup__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __running__(self):
    return True

  def run(self):
    while self.__running__():
      [token, log_name, payload_map, stage_to_expected_num_files] = self.queue.get()
      m = util.parse_file_name(log_name)
      body = self.__get_log__(log_name)
      for payload in body["payloads"]:
        if "log" in payload:
          child_identifier = tuple(payload["log"])
        else:
          c = util.parse_file_name(payload["Records"][0]["s3"]["object"]["key"])
          if "extra_params" in payload["Records"][0]["s3"]:
            c = {**c, **payload["Records"][0]["s3"]["extra_params"]}
          child_identifier = (c["prefix"], c["bin"], c["file_id"])

          if c["prefix"] not in stage_to_expected_num_files:
            stage_to_expected_num_files[c["prefix"]] = (c["num_bins"], c["num_files"], c["num_bins"] * c["num_files"])
        payload_map[child_identifier[0]][child_identifier] = payload


class Task(threading.Thread):
  def __init__(self, bucket_name, job, timeout, invoker_queue, logger_queue, tokens, params):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.check_time = time.time()
    self.expected_logs = set([(0, 1, 1)])
    self.actual_logs = set()
    self.job = job
    self.key = job.key
    self.logger_queue = logger_queue
    self.params = params
    self.payload_map = { 0: {} }
    self.invoker_queue = invoker_queue
    self.processed = set()
    self.running = True
    self.stage = 0
    self.stage_to_expected_num_files = {0: (0, 1, 1)}
    self.timeout = timeout
    self.token = job.key
    self.tokens = tokens
    self.__setup__()

  def __get_objects__(self, stage):
    return self.bucket.objects.filter(Prefix="{0:d}/{1:s}/".format(stage, self.token))

  def __running__(self):
    return True

  def __setup__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __upload__(self):
    [key, _, _] = upload.upload(self.job.destination_bucket, self.job.key, self.job.source_bucket, max(self.job.pause[0], 0))
    self.key = key
    self.token = key.split("/")[1]
    self.tokens.add(self.token)
    self.payload_map[0][(0, 1, 1)] = payload(self.job.destination_bucket, self.key)
    self.check_time = time.time()

  def check_for_updates(self):
    for stage in range(self.stage, len(self.params["pipeline"])):
      if stage not in self.payload_map:
        self.payload_map[stage] = {}
      logs = list(map(lambda obj: obj.key, self.__get_objects__(stage)))
      lset = set(logs)
      if stage not in self.stage_to_expected_num_files and len(logs) > 0:
        m = util.parse_file_name(logs[0])
        self.stage_to_expected_num_files[stage] = (m["num_bins"], m["num_files"], m["num_bins"] * m["num_files"])

      if stage in self.stage_to_expected_num_files:
        if stage == self.stage and len(logs) == self.stage_to_expected_num_files[stage][2]:
          self.stage += 1
          self.check_time = time.time()
      missing_logs = lset.difference(self.processed)
      if len(missing_logs) > 0:
        print(self.token, "Stage", stage, "Adding", len(missing_logs), "Logs")
      for log in missing_logs:
        m = util.parse_file_name(log)
        self.actual_logs.add((m["prefix"], m["bin"], m["file_id"]))
        self.logger_queue.put([self.token, log, self.payload_map, self.stage_to_expected_num_files])
      self.processed = self.processed.union(missing_logs)

  def get_missing_logs(self):
    for stage in self.stage_to_expected_num_files:
      [num_bins, num_files, _] = self.stage_to_expected_num_files[stage]
      for bin_id in range(1, num_bins + 1):
        for file_id in range(1, num_files + 1):
          self.expected_logs.add((stage, bin_id, file_id))
    missing_logs = self.expected_logs.difference(self.actual_logs)
    print(self.token, "Stage", self.stage, "Missing", len(missing_logs), "Expected", len(self.expected_logs))
    return missing_logs

  def invoke(self, name, payload, unpause):
    if unpause:
      payload["execute"] = 0
    self.invoker_queue.put([name, payload])

  def run(self):
    sleep = self.job.start_time - time.time()
    if sleep > 0:
      time.sleep(sleep)
    if self.job.upload:
      self.__upload__()

    print(self.token, "Starting stage", self.stage)
    while self.__running__() and self.stage < len(self.params["pipeline"]):
      self.check_for_updates()
      ctime = time.time()
      if self.job.pause[0] != -1 and self.job.pause[0] < ctime and ctime < self.job.pause[1]:
        sleep = self.job.pause[1] - ctime
        print(self.token, "Sleeping for", sleep, "seconds")
        time.sleep(self.job.pause[1] - ctime)
      else:
        if (ctime - self.check_time) > self.timeout:
          log_identifiers = self.get_missing_logs()
          count = 0
          for identifier in log_identifiers:
            stage = identifier[0]
            name = self.params["pipeline"][stage]["name"]
            if stage in self.payload_map and identifier in self.payload_map[stage]:
              self.invoke(name, self.payload_map[stage][identifier], self.job.pause[1] < ctime)
              count += 1
          print(self.token, "Invoked", count, "Payloads")
          self.check_time = time.time()
        time.sleep(5)

    print(self.token, "Done processing.")


class Scheduler:
  def __init__(self, policy, timeout, params):
    self.invokers = []
    self.loggers = []
    self.max_tasks = 1000
    self.messages = {}
    self.next_job_id = 0
    self.params = params
    self.logger_queue = queue.Queue()
    self.invoker_queue = queue.Queue()
    self.policy = policy
    self.prefixes = set()
    self.running = True
    self.__setup__()
    self.tasks = []
    self.timeout = timeout
    self.tokens = set()

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")

  def __add_invoker__(self, i):
    self.invokers.append(Invoker(self.invoker_queue, i, self.params))
    self.invokers[-1].start()

  def __add_task__(self, job):
    self.tasks.append(Task(self.params["log"], job, self.timeout, self.invoker_queue, self.logger_queue, self.tokens, self.params))

  def __add_logger__(self, i):
    self.loggers.append(Logger(self.params["log"], self.logger_queue))
    self.loggers[-1].start()

  def __check_tasks__(self):
    self.__get_messages__(self.log_queue)
    tokens = self.messages[0].keys() if 0 in self.messages else []
    for token in tokens:
      if token not in self.tokens:
        job = Job("", self.params["bucket"], token, float(token.split("-")[0]))
        self.__add_task__(job)
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

  def add_jobs(self, jobs):
    for job in jobs:
      job.upload = True
      self.__add_task__(job)
      self.tasks[-1].start()

  def listen(self, num_invokers, num_loggers):
    for i in range(num_invokers):
      self.__add_invoker__(i)
    for i in range(num_loggers):
      self.__add_logger__(i)
    print("Listening")
    while self.__running__():
      self.__check_tasks__()
    print("Done Listening")
    for task in self.tasks:
      task.join()


def run(policy, timeout, params):
  scheduler = Scheduler(policy, timeout, params)
  scheduler.listen(params["num_invokers"], params["num_loggers"])


def simulation_deadline(jobs: List[Job], expected_job_duration: float, max_num_jobs: int, key_func):
  timestamps = []
  for i in range(len(jobs)):
    start_time = min(jobs[i].start_time, jobs[i].deadline - expected_job_duration)
    end_time = min(jobs[i].deadline, start_time + expected_job_duration)
    timestamps.append((start_time, i, jobs[i], 1))
    timestamps.append((end_time, i, jobs[i], -1))
  timestamps = sorted(timestamps, key=lambda t: t[0])

  running = {}
  paused = {}
  i = 0
  while i < len(timestamps):
    timestamp = timestamps[i]
    if timestamp[1] in paused:
      # We can't finish the task. It's paused.
      assert(timestamp[3] == -1)
      timestamps.pop(i)
    else:
      if timestamp[3] == 1:
        if len(running) >= max_num_jobs:
          # We hit max number of current jobs. Pause the one that has to finish last.
          running_keys = sorted(list(running.keys()), key=lambda k: key_func(running[k][2]))
          last_key = running_keys[-1]
          last_job = running[last_key][2]
          if key_func(last_job) > key_func(timestamp[2]):
            last_job.pause[0] = timestamp[0]
            paused[last_key] = running[last_key]
            del running[last_key]
            running[timestamp[1]] = timestamp
          else:
            timestamp[2].pause[0] = timestamp[0]
            paused[timestamp[1]] = timestamp
        else:
          running[timestamp[1]] = timestamp
      else:
        if timestamp[1] in running:
          del running[timestamp[1]]
        if len(paused) > 0:
          # We have room to resume old jobs. Pick the one that needs to finish first.
          paused_keys = sorted(list(paused.keys()), key=lambda k: key_func(paused[k][2]))
          first_key = paused_keys[0]
          first_timestamp = paused[first_key]
          first_job = first_timestamp[2]
          first_job.pause[1] = timestamp[0]
          del paused[first_key]
          running[first_timestamp[1]] = first_timestamp
          paused_time = first_job.pause[1] - first_job.pause[0]
          end_time = min(first_job.deadline, first_job.start_time + expected_job_duration) + paused_time
          timestamp = (end_time, first_key, first_job, -1)
          assert(end_time >= timestamp[0])
          found = False
          j = i + 1
          while j < len(timestamps) and not found:
            if timestamp[0] < timestamps[j][0]:
              timestamps.insert(j, timestamp)
              found = True
            j += 1
          if not found:
            timestamps.append(timestamp)
      i += 1

  orders = list(map(lambda job: (job, job.start_time), jobs))
  return orders


def simulation_order(jobs: List[Job], policy: str, expected_job_duration: float, max_num_jobs: int):
  orders: List[Order] = []
  if policy == "fifo":
    jobs = sorted(jobs, key=lambda job: job.start_time)
    orders = list(map(lambda job: (job, job.start_time), jobs))
  elif policy == "priority":
    orders = simulation_deadline(jobs, expected_job_duration, max_num_jobs, lambda k: -1 * k.priority)
  elif policy == "robin":
    # We assume all jobs come in at the same time to simulate round robin
    for i in range(len(jobs)):
      orders.append((jobs[i], 2*i))
  elif policy == "deadline":
    orders = simulation_deadline(jobs, expected_job_duration, max_num_jobs, lambda k: k.deadline)
  else:
    raise Exception("Policy", policy, "not implemented")
  return orders


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

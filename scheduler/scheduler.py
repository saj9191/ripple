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
  def __init__(self, payload_queue, thread_id, params):
    super(Invoker, self).__init__()
    self.payload_queue = payload_queue
    self.running = True
    self.thread_id = thread_id
    self.__setup_client__(params)

  def __setup_client__(self, params):
    s3 = boto3.resource("s3")
    self.client = util.setup_client("lambda", params)

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
    print("Invoker", self.thread_id, "Start")
    while self.__running__():
      name, payload = self.payload_queue.get()
      self.__invoke__(name, payload)


class Task(threading.Thread):
  def __init__(self, bucket_name, job, timeout, params, payload_queue, tokens):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.key_to_payload = {}
    self.check_time = time.time()
    self.found = {}
    self.job = job
    self.params = params
    self.payload_queue = payload_queue
    self.processed = set()
    self.running = True
    self.stage = 0
    self.timeout = timeout
    self.token = job.key
    self.tokens = tokens
    self.__setup__()

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

  def __get_object__(self, prefix):
    objs = list(self.s3.Bucket(self.params["log"]).objects.filter(Prefix=prefix))
    assert len(objs) > 0, "Prefix is {0:s}".format(prefix)
    obj = objs[0]
    content = obj.get()["Body"].read().decode("utf-8")
    return json.loads(content)

  def __get_objects__(self, stage):
    objs = self.bucket.objects.filter(Prefix=str(stage) + "/" + self.token)
    return objs

  def __setup__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __invoke__(self, name, payload):
    self.payload_queue.put([name, payload])

  def __running__(self):
    return True


  def __find_payloads__(self, actual_logs, max_bin, max_file):
    if self.stage == 0:
      return [payload(self.bucket_name, self.job.key)]
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

    m["timestamp"] = float(self.token.split("-")[0])
    m["nonce"] = int(self.token.split("-")[1])
    m["prefix"] = self.stage - 1
    prefixes = set()

    print(self.token, "Stage", self.stage, "Looking for", len(missing), "files")
    for (s, b, f) in missing:
      if self.stage == 1: # Pair train
        assert (s-1, 1, 1) in parent_logs, (s-1, 1, 1)
        assert len(parent_logs) == 1, len(parent_logs)
        m["bin"] = m["num_bins"] = m["file_id"] = m["num_files"] = 1
      elif self.stage == 2: # KNN
        assert (s-1, b, 1) in parent_logs, (s-1, b, 1)
        assert max_parent_file == 1, max_parent_file
        m["bin"] = b
        m["file_id"] = m["num_files"] = 1
      elif self.stage == 3: # combine
        assert (s-1, b, f) in parent_logs, (s-1, b, f)
        assert max_parent_bin == max_bin, (max_parent_bin, max_bin)
        assert max_parent_file == max_file, (max_parent_file, max_file)
        m["bin"] = b
        m["file_id"] = f
      elif self.stage == 4: # second combine
        assert (s-1, b, f) in parent_logs, (s-1, b, f)
        assert max_bin == 1, max_bin
        assert max_file == max_parent_bin, (max_file, max_parent_bin)
        m["bin"] = b
        m["num_bins"] = max_parent_bin
        m["file_id"] = f
        m["num_files"] = max_file
      elif self.stage == 5: # draw
        m["bin"] = m["num_bins"] = m["file_id"] = m["num_files"] = 1
        assert (s-1, 1, 1) in parent_logs, (s-1, 1, 1)
        assert max_parent_bin > 1, max_parent_bin
        assert max_parent_file > 1, max_parent_file

      name = util.file_name(m)
      prefix = "-".join(name.split("-")[:-3]) + "-"
#      print("Missing", (s, b, f), "Prefix", prefix)
      prefixes.add(prefix)

    payloads = []
#    print("missing", list(missing)[0])
    for prefix in prefixes:
      body = self.__get_object__(prefix)
      for payload in body["payloads"]:
        if "log" in payload:
          t = tuple(payload["log"])
          if t in missing:
            payloads.append(payload)
            missing.remove(t)
        else:
          m = util.parse_file_name(payload["Records"][0]["s3"]["object"]["key"])
          t = (s, m["bin"], m["file_id"])
          if t in missing:
            payloads.append(payload)
            missing.remove(t)

    return payloads

  def __upload__(self):
    [key, _, _] = upload.upload(self.job.destination_bucket, self.job.key, self.job.source_bucket, max(self.job.pause[0], 0))
    self.token = key.split("/")[1]
    self.tokens.add(self.token)
    self.check_time = time.time()

  def run(self):
    sleep = self.job.start_time - time.time()
    if sleep > 0:
      time.sleep(sleep)
    if self.job.upload:
      self.__upload__()
    print(self.token, "Starting stage", self.stage)
    while self.__running__() and self.stage < len(self.params["pipeline"]):
      [actual_logs, max_bin, max_file, expected_num_bins] = self.__current_logs__(self.__get_objects__(self.stage))
      self.found[self.stage] = actual_logs
      if len(actual_logs) == expected_num_bins:
        # We have all the payloads for this stage
        self.stage += 1
        print(self.token, "Starting stage", self.stage)
      else:
        ctime = time.time()
        if ctime - self.check_time > self.timeout and (self.job.pause[0] == -1 or ctime < self.job.pause[0] or ctime >= self.job.pause[1]):
          payloads = self.__find_payloads__(actual_logs, max_bin, max_file)
          print(self.token, "Stage", self.stage, "Reinvoking", len(payloads), "payloads")
          for i in range(len(payloads)):
            payload = payloads[i]
            if self.job.pause[1] != -1 and ctime >= self.job.pause[1]:
              payload["execute"] = 0
            name = self.params["pipeline"][self.stage]["name"]
            self.__invoke__(name, payload)
          self.check_time = time.time()
      time.sleep(5)

    print("Done processing", self.token)
    self.running = (self.stage < len(self.params["pipeline"]))


class Scheduler:
  def __init__(self, policy, timeout, params):
    self.invokers = []
    self.max_tasks = 1000
    self.messages = {}
    self.next_job_id = 0
    self.params = params
    self.payload_queue = queue.Queue()
    self.policy = policy
    self.prefixes = set()
    self.running = True
    self.__setup__()
    self.tasks = []
    self.timeout = timeout
    self.tokens = set()

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")

  def __add_task__(self, job):
    self.tasks.append(Task(self.params["log"], job, self.timeout, self.params, self.payload_queue, self.tokens))

  def __add_invoker__(self, i):
    self.invokers.append(Invoker(self.payload_queue, i, self.params))
    self.invokers[-1].start()

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

  def listen(self):
    for i in range(10):
      self.__add_invoker__(i)
    print("Listening")
    while self.__running__():
      self.__check_tasks__()
    print("Done Listening")
    for task in self.tasks:
      task.join()


def run(policy, timeout, params):
  scheduler = Scheduler(policy, timeout, params)
  scheduler.listen()


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
            paused[timestamp[1]] = timestamp
        else:
          running[timestamp[1]] = timestamp
      else:
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

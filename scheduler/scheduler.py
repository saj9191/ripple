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
from typing import Any, Dict, List, MutableSet, Tuple
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
Token = Tuple[int, int, int, int, int, int]


class Queue(threading.Thread):
  bucket_name: str
  finished_tasks: MutableSet[Token]
  pending_job_tokens: MutableSet[str]
  logger_queue: queue.Queue

  def __init__(self, bucket_name: str, logger_queue, finished_tasks, pending_job_tokens, processed_logs):
    super(Queue, self).__init__()
    self.bucket_name = bucket_name
    self.finished_tasks = finished_tasks
    self.logger_queue = logger_queue
    self.pending_job_tokens = pending_job_tokens
    self.processed_logs = processed_logs
    self.__setup_connections__()

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __fetch_objects__(self):
    while True:
      try:
        objects = self.bucket.objects.all()
        return objects
      except Exception as e:
        raise e
        time.sleep(random.randint(0, 3))

  def __get_objects__(self):
    objects = self.__fetch_objects__()
    for obj in objects:
      key = obj.key
      if key not in self.processed_logs:
        self.processed_logs.add(key)
        m = util.parse_file_name(key)
        prefix: int = m["prefix"]
        token: str = "{0:f}-{1:d}".format(m["timestamp"], m["nonce"])
        identifier: Token = (token, m["prefix"], m["bin"], m["num_bins"], m["file"], m["num_files"])
        self.pending_job_tokens.add(token)
        self.finished_task.add(identifier)
        self.logger_queue.put([key, identifier])

  def __running__(self):
    return True

  def run(self):
    while self.__running__():
      self.__get_objects__()


class Logger(threading.Thread):
  def __init__(self, bucket_name, logger_queue, payload_map):
    super(Logger, self).__init__()
    self.bucket_name = bucket_name
    self.logger_queue = logger_queue
    self.__setup_connections_()

  def __running__(self):
    return True

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")

  def __get_log__(self, name):
    while not util.object_exists(self.s3, self.bucket_name, name):
      time.sleep(0.5)

    while True:
      try:
        obj = self.s3.Object(self.bucket_name, name)
        body = json.loads(obj.get()["Body"].read().decode("utf-8"))
        return body
      except Exception as e:
        raise e
        time.sleep(random.randint(0, 3))

  def run(self):
    while self.__running__():
      [key, identifier] = self.logger_queue.get()
      body = self.__get_log__(key)
      for payload in body["payloads"]:
        token = identifier[0]
        if "log" in payload:
          child_identifier = tuple(payload["log"])
        else:
          c = util.parse_file_name(payload["Records"][0]["s3"]["object"]["key"])
          if "extra_params" in payload["Records"][0]["s3"]:
            c = {**c, **payload["Records"][0]["s3"]["extra_params"]}
          child_identifier = (token, c["prefix"], c["bin"], c["num_bins"], c["file_id"], c["num_files"])
        self.payload_map[child_identifier] = payload


class Invoker(threading.Thread):
  def __init__(self, invoker_queue):
    super(Invoker, self).__init__()
    self.invoker_queue = invoker_queue
    self.running = True
    self.__setup_connections__()

  def __invoke__(self, name, payload):
    while True:
      try:
        response = self.client.invoke(
          FunctionName=name,
          InvocationType="Event",
          Payload=json.JSONEncoder().encode(payload)
        )
        assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)
        return
      except Exception as e:
        raise e
        time.sleep(random.randint(0, 3))

  def __running__(self):
    return True

  def __setup_connections__(self):
    self.client = boto3.client("lambda")

  def run(self):
    while self.__running__():
      name, payload = self.invoker_queue.get()
      self.__invoke__(name, payload)

class Task(threading.Thread):
  def __init__(self, bucket_name, job, timeout, invoker_queue, job_tokens, finished_tasks, payload_map, pipeline):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.check_time = time.time()
    self.expected_logs = set()
    self.finished_tasks = finished_tasks
    self.invoker_queue = invoker_queue
    self.job = job
    self.key = job.key
    self.logger_queue = logger_queue
    self.payload_map = payload_map
    self.pipeline = pipeline
    self.running = True
    self.stage = 0
    self.timeout = timeout
    self.token = job.key
    self.job_tokens = job_tokens
    self.__setup_connections__()

  def __running__(self):
    return True

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __upload__(self):
    [key, _, _] = upload.upload(self.job.destination_bucket, self.job.key, self.job.source_bucket, max(self.job.pause[0], 0))
    self.key = key
    self.token = key.split("/")[1]
    self.job_tokens.add(tokens)
    self.expected_logs.add([self.token, 0, 1, 1, 1, 1])
    self.check_time = time.time()

  def __stage_tasks__(self, stage):
    return list(filter(lambda t: t[0] == self.token and t[1] == stage, self.finished_tasks))

  def check_for_updates(self):
    stage_tasks = self.__stage_tasks__(self.stage)
    if len(stage_tasks) > 0:
      num_files = stage_tasks[0][3] * stage_tasks[0][5]
    else:
      num_files = None

    if num_files is not None and len(stage_tasks) == num_files:
      self.stage += 1
      self.check_time = time.time()

  def get_missing_logs(self):
    for stage in range(self.stage, len(self.pipeline)):
      stage_tasks = self.__stage_tasks__(stage)
      if stage_tasks[0] not in self.expected_logs:
        num_bins = stage_tasks[0][3]
        num_files = stage_tasks[0][5]
        for bin_id in range(1, num_bins + 1):
          for file_id in range(1, num_files + 1):
            self.expected_logs.add((self.token, stage, bin_id, num_bins, file_id, num_files))
    missing_logs = self.expected_logs.difference(self.finished_tasks)
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

    print(self.token, "Starting stage", self.stage, "Timeout", self.timeout)
    while self.__running__() and self.stage < self.pipeline_length:
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
            if identifier in self.payload_map:
              self.invoke(name, self.payload_map[identifier], self.job.pause[1] < ctime)
              count += 1
          self.check_time = time.time()

    print(self.token, "Done processing.")

class Scheduler:
  def __init__(self, policy, timeout, params):
    self.bucket_name = params["bucket"]
    self.log_name = params["log"]
    self.finished_tasks = set()
    self.invoker_queue = queue.Queue()
    self.invokers = []
    self.job_tokens = set()
    self.logger_queue = queue.Queue()
    self.loggers = []
    self.max_tasks = 1000
    self.payload_map = {}
    self.pipeline = params["pipeline"]
    self.pending_job_tokens = set()
    self.policy = policy
    self.processed_logs = set()
    self.queues = []
    self.running = True
    self.tasks = []
    self.timeout = timeout

  def __add_invoker__(self):
    self.invokers.append(Invoker(self.invoker_queue))
    self.invokers[-1].start()

  def __add_task__(self, job):
    self.tasks.append(Task(self.log_name, job, self.timeout, self.invoker_queue, self.job_tokens, self.finished_tasks, self.payload_map, self.pipeline))
    self.tasks[-1].start()

  def __add_queue__(self):
    self.queues.append(Queue(self.log_name, self.logger_queue, self.finished_tasks, self.pending_job_tokens, self.processed_logs))
    self.queues[-1].start()

  def __add_logger__(self):
    self.loggers.append(Logger(self.log_name, self.logger_queue, self.payload_map))
    self.loggers[-1].start()

  def __check_tasks__(self):
    for token in self.pending_job_tokens:
      self.pending_job_tokens.remove(token)
      if token not in self.job_tokens:
        self.job_tokens.add(token)
        job = Job("", self.bucket_name, token, float(token.split("-")[0]))
        self.__add_task__(job)

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running:
        self.tasks[i].join()
        self.tasks.pop(i)
      else:
        i += 1

  def __running__(self):
    return self.running

  def add_jobs(self, jobs):
    for job in jobs:
      job.upload = True
      self.__add_task__(job)
      self.tasks[-1].start()

  def listen(self, num_invokers, num_loggers):
    for i in range(num_invokers):
      self.__add_invoker__()
    for i in range(num_loggers):
      self.__add_logger__()
    for i in range(5):
      self.__add_queue__()

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

  running: Dict[int, Any] = {}
  paused: Dict[int, Any] = {}
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
  params["s3"] = database.S3(params)
  run(args.policy, args.timeout, params)


if __name__ == "__main__":
  main()

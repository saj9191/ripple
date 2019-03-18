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
  id: int
  finished_tasks: MutableSet[Token]
  logger_queue: queue.Queue

  def __init__(self, id: int, bucket_name: str, logger_queue, finished_tasks, pending_job_tokens, processed_logs, tokens, prefixes, key_queue, find_queue):
    super(Queue, self).__init__()
    self.bucket_name = bucket_name
    self.finished_tasks = finished_tasks
    self.tokens = tokens
    self.key_queue = key_queue
    self.logger_queue = logger_queue
    self.marker = ""
    self.pending_job_tokens = pending_job_tokens
    self.find_queue = find_queue
    self.prefixes = prefixes
    self.processed_logs = processed_logs
    self.thread_id = id
    self.__setup_connections__()

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")
    self.client = boto3.client("s3")

  def __fetch_objects__(self, file_name):
    while True:
      try:
        response = self.client.list_objects(Bucket=self.bucket_name, Marker=file_name, MaxKeys=500)
        contents = response["Contents"] if "Contents" in response else []
        return contents
      except Exception as e:
        print(e)
        time.sleep(random.uniform(0, 1))

  def __get_objects__(self):
    if self.prefixes:
      file_name = self.prefixes + "/"
    else:
      identifier = self.find_queue.get()
      parts = identifier[0].split("-")
      m = {
        "prefix": identifier[1],
        "timestamp": float(parts[0]),
        "nonce": int(parts[1]),
        "bin": identifier[2],
        "num_bins": identifier[3],
        "file_id": identifier[4],
        "execute": 0,
        "suffix": "0",
        "num_files": identifier[5],
        "ext": "log",
      }
      file_name = util.file_name(m)

    objects = self.__fetch_objects__(file_name)

    count = 0
    found = False
    for obj in objects:
      key = obj["Key"]
      found |= (key == file_name)
      if key not in self.processed_logs:
        count += 1
        self.key_queue.put(key)

    if not found and not self.prefixes:
      self.find_queue.put(identifier)

  def __running__(self):
    return True

  def run(self):
    print("Queue started. Monitoring ", self.bucket_name, "ID is", self.thread_id, "Prefixes are", self.prefixes)
    while self.__running__():
      st = time.time()
      self.__get_objects__()
      et = time.time()
      if et - st > 1:
        print("Queue::run", et - st, len(self.processed_logs))
      time.sleep(random.uniform(0, 1))


class Logger(threading.Thread):
  def __init__(self, bucket_name, logger_queue, finished_tasks, payload_map, find_queue):
    super(Logger, self).__init__()
    self.bucket_name = bucket_name
    self.finished_tasks = finished_tasks
    self.logger_queue = logger_queue
    self.payload_map = payload_map
    self.find_queue = find_queue
    self.__setup_connections__()

  def __running__(self):
    return True

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")

  def __get_log__(self, name):
    while not util.object_exists(self.s3, self.bucket_name, name):
      time.sleep(random.randint(0, 3))

    while True:
      try:
        obj = self.s3.Object(self.bucket_name, name)
        body = json.loads(obj.get()["Body"].read().decode("utf-8"))
        return body
      except Exception as e:
        time.sleep(random.uniform(0, 1))

  def __process_logs__(self):
    [key, identifier] = self.logger_queue.get()
    body = self.__get_log__(key)
    for payload in body["payloads"]:
      token = identifier[0]
      s3 = payload["Records"][0]["s3"]
      for ancestry_identifier in s3["ancestry"]:
        self.finished_tasks.add(tuple(ancestry_identifier))
      if "log" in payload:
        child_identifier = tuple(payload["log"])
      else:
        c = util.parse_file_name(s3["object"]["key"])
        if "extra_params" in s3:
          c = {**c, **s3["extra_params"]}
        child_identifier = (token, c["prefix"], c["bin"], c["num_bins"], c["file_id"], c["num_files"])
      self.find_queue.put(child_identifier)
      assert(child_identifier[2] <= child_identifier[3] and child_identifier[4] <= child_identifier[5])
      self.payload_map[token][child_identifier] = payload

  def run(self):
    while self.__running__():
      st = time.time()
      self.__process_logs__()
      et = time.time()
      if et - st > 1:
        print("Logger::run", et - st)


class Invoker(threading.Thread):
  def __init__(self, invoker_queue, region, key_queue):
    super(Invoker, self).__init__()
    self.invoker_queue = invoker_queue
    self.region = region
    self.running = True
    self.key_queue = key_queue
    self.__setup_connections__()

  def __invoke__(self, name, payload, identifier):
    while True:
      try:
        parts = identifier[0].split("-")
        m = {
          "prefix": identifier[1],
          "timestamp": float(parts[0]),
          "nonce": int(parts[1]),
          "bin": identifier[2],
          "num_bins": identifier[3],
          "file_id": identifier[4],
          "execute": 0,
          "suffix": "0",
          "num_files": identifier[5],
          "ext": "log",
        }
        file_name = util.file_name(m)
        if util.object_exists(self.s3, "maccoss-log-east-1", file_name):
          self.key_queue.put(file_name)
          return
        response = self.client.invoke(
          FunctionName=name,
          InvocationType="Event",
          Payload=json.JSONEncoder().encode(payload)
        )
        assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)
        return
      except Exception as e:
        print("sigh", e)
        time.sleep(random.uniform(0, 1))

  def __running__(self):
    return True

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")
    self.client = boto3.client("lambda", region_name=self.region)

  def run(self):
    while self.__running__():
      name, payload, identifier = self.invoker_queue.get()
      self.__invoke__(name, payload, identifier)

class Task(threading.Thread):
  def __init__(self, bucket_name, job, timeout, invoker_queue, job_tokens, finished_tasks, payload_map, pipeline, find_queue):
    super(Task, self).__init__()
    self.bucket_name = bucket_name
    self.expected_logs = set()
    self.find_queue = find_queue
    self.finished_tasks = finished_tasks
    self.invoker_queue = invoker_queue
    self.job = job
    self.key = job.key
    self.offset = 20
    self.payload_map = payload_map
    self.pipeline = pipeline
    self.running = True
    self.stage = 0
    self.timeout = timeout
    self.token = job.key
    self.job_tokens = job_tokens
    self.__setup_connections__()

  def __running__(self):
    return self.running

  def __setup_connections__(self):
    self.s3 = boto3.resource("s3")
    self.bucket = self.s3.Bucket(self.bucket_name)

  def __upload__(self):
    [key, _, _] = upload.upload(self.job.destination_bucket, self.job.key, self.job.source_bucket, max(self.job.pause[0], 0))
    self.key = key
    self.token = key.split("/")[1]
    self.payload_map[self.token] = {}
    self.job_tokens.add(self.token)

  def __stage_tasks__(self, stage):
    finished_tasks = self.finished_tasks.intersection(self.expected_logs)
    return [x for x in finished_tasks if x[1] == stage]

  def check_for_updates(self):
    stage_tasks = self.__stage_tasks__(self.stage)
    if len(stage_tasks) > 0:
      num_files = stage_tasks[0][3] * stage_tasks[0][5]
    else:
      num_files = None

    if num_files is not None and len(stage_tasks) == num_files:
      self.stage += 1
      self.check_time = time.time() + random.randint(-self.offset, self.offset)

  def get_missing_logs(self):
    self.expected_logs = self.expected_logs.union(set(self.payload_map[self.token].keys()))
    for stage in range(self.stage, len(self.pipeline)):
      stage_tasks = self.__stage_tasks__(stage)
      if len(stage_tasks) > 0 and stage_tasks[0] not in self.expected_logs:
        num_bins = stage_tasks[0][3]
        num_files = stage_tasks[0][5]
        for bin_id in range(1, num_bins + 1):
          for file_id in range(1, num_files + 1):
            assert(bin_id <= num_bins and file_id <= num_files)
            t = (self.token, stage, bin_id, num_bins, file_id, num_files)
            if t not in self.expected_logs:
              self.expected_logs.add(t)
              self.find_queue.add(t)
    missing= self.expected_logs.difference(self.finished_tasks)
    return list(missing)

  def invoke(self, name, payload, identifier, unpause):
    if unpause:
      payload["execute"] = 0
    self.invoker_queue.put([name, payload, identifier])

  def __process__(self):
    self.check_for_updates()
    ctime = time.time()
    if self.job.pause[0] != -1 and self.job.pause[0] < ctime and ctime < self.job.pause[1]:
      sleep = self.job.pause[1] - ctime
      print(self.token, "Sleeping for", sleep, "seconds")
      time.sleep(self.job.pause[1] - ctime)
      print(self.token, "Waking up")
      identifier = (self.token, 0, 1, 1, 1, 1)
      name = self.pipeline[0]["name"]
      self.invoke(name, self.payload_map[self.token][identifier], self.job.pause[1] < ctime)
      self.check_time = time.time() + random.randint(-self.offset, self.offset)
    else:
      if (ctime - self.check_time) > self.timeout:
        log_identifiers = self.get_missing_logs()
        random.shuffle(log_identifiers)
        count = 0
        for identifier in log_identifiers[:10]:
          if identifier in self.payload_map[self.token]:
            name = self.pipeline[identifier[1]]["name"]
    #        self.invoke(name, self.payload_map[self.token][identifier], identifier, self.job.pause[1] != -1 and self.job.pause[1] < ctime)
            count += 1
        if count > 0:
          print(self.token, "Re-invoked", count, "payloads. Missing ", list(log_identifiers)[0])
        self.check_time = time.time() + random.randint(-self.offset, self.offset)

  def run(self):
    sleep = self.job.start_time - time.time()
    if sleep > 0:
      time.sleep(sleep)
    if self.job.upload:
      self.__upload__()
    identifier = (self.token, 0, 1, 1, 1, 1)
#    self.find_queue.put(identifier)
    self.expected_logs.add(identifier)
    self.payload_map[self.token][identifier] = payload(self.job.destination_bucket, self.key)
    self.check_time = time.time() + random.randint(-self.offset, self.offset)

    print(self.token, "Starting stage", self.stage, self.job.pause)
    while self.__running__() and self.stage < len(self.pipeline):
      st = time.time()
      self.__process__()
      et = time.time()
      if et - st > 1:
        print("Task::run", et - st)
      time.sleep(random.uniform(0, 1))
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
    self.pending_job_tokens = queue.Queue()
    self.policy = policy
    self.processed_logs = set()
    self.queues = []
    self.key_queue = queue.Queue()
    self.region = params["region"]
    self.running = True
    self.tasks = []
    self.timeout = timeout
    self.tokens = []
    self.find_queue = queue.Queue()

  def __add_invoker__(self):
    self.invokers.append(Invoker(self.invoker_queue, self.region, self.key_queue))
    self.invokers[-1].start()

  def __add_task__(self, job):
    self.tasks.append(Task(self.log_name, job, self.timeout, self.invoker_queue, self.job_tokens, self.finished_tasks, self.payload_map, self.pipeline, self.find_queue))
    self.tasks[-1].start()

  def __add_queue__(self, id, prefixes):
    self.queues.append(Queue(id, self.log_name, self.logger_queue, self.finished_tasks, self.pending_job_tokens, self.processed_logs, self.tokens, prefixes, self.key_queue, self.find_queue))
    self.queues[-1].start()

  def __add_logger__(self):
    self.loggers.append(Logger(self.log_name, self.logger_queue, self.finished_tasks, self.payload_map, self.find_queue))
    self.loggers[-1].start()

  def __check_tasks__(self):
    key = self.key_queue.get()
    m = util.parse_file_name(key)
    token: str = "{0:f}-{1:d}".format(m["timestamp"], m["nonce"])
    identifier: Token = (token, m["prefix"], m["bin"], m["num_bins"], m["file_id"], m["num_files"])
    if key not in self.processed_logs:
      self.processed_logs.add(key)
      self.finished_tasks.add(identifier)
      self.logger_queue.put([key, identifier])

      if token not in self.payload_map:
        self.payload_map[token] = {}
        self.tokens.append(token)
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

  def listen(self, num_invokers, num_loggers):
    for i in range(num_invokers):
      self.__add_invoker__()
    for i in range(num_loggers):
      self.__add_logger__()

    for i in range(20): 
      self.__add_queue__(i, None)

    self.__add_queue__(i, "0")
   # for i in range(10):
   #   self.__add_queue__(i, [4, 5])

   # for i in range(5):
   #   self.__add_queue__(i, [2, 3])

#    for i in range(3):
#      self.__add_queue__(i, [1])

    print("Listening")
    while self.__running__():
      self.__check_tasks__()
    print("Done Listening")
    for task in self.tasks:
      task.join()


def run(policy, timeout, params):
 scheduler = Scheduler(policy, timeout, params)
 params["num_invokers"] = 10
 params["num_loggers"] = 10
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
  #setup.process_functions(params)
  params["s3"] = database.S3(params)
  run(args.policy, args.timeout, params)


if __name__ == "__main__":
  main()

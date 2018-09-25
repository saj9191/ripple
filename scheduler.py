import argparse
import botocore
import json
import queue
import threading
import time
import util


class Item:
  def __init__(self, scheduler, priority, prefix, job_id, payload):
    self.priority = priority
    self.prefix = prefix
    self.payload = payload
    self.job_id = job_id
    self.scheduler = scheduler

  def __lt__(self, other):
    if self.scheduler == "robin":
      return self.priority < other.priority
    elif self.scheduler == "fifo":
      return [self.job_id, self.priority] < [other.job_id, other.priority]


class PriorityQueue:
  def __init__(self, scheduler):
    self.scheduler = scheduler
    if scheduler == "fifo":
      self.queue = queue.PriorityQueue()
    elif scheduler == "robin":
      self.queue = []
      self.index = 0
    else:
      raise Exception("Not Implemented")

  def put(self, item):
    if self.scheduler == "fifo":
      self.queue.put(item)
    elif self.scheduler == "robin":
      while len(self.queue) <= item.job_id:
        self.queue.append(queue.PriorityQueue())
      self.queue[item.job_id].put(item)
    else:
      raise Exception("Not Implemented")

  def get(self):
    if self.scheduler == "fifo":
      item = self.queue.get(timeout=0)
      return item
    elif self.scheduler == "robin":
      count = 0
      while count < len(self.queue):
        index = self.index
        self.index = (self.index + 1) % len(self.queue)
        try:
          item = self.queue[index].get(timeout=0)
          return item
        except queue.Empty:
          count += 1
      raise queue.Empty
    else:
      raise Exception("Not Implemented")


class Worker(threading.Thread):
  def __init__(self, worker_id, scheduler, pending, q, params, condition, results):
    super(Worker, self).__init__()
    self.worker_id = worker_id
    self.pending = pending
    self.queue = q
    self.running = True
    self.params = dict(params)
    self.condition = condition
    self.results = results
    self.index = 0
    self.scheduler = scheduler

  def get_item(self):
    if self.scheduler == "robin":
      count = 0
      while count < len(self.pending):
        try:
          item = self.pending[self.index].get(timeout=0)
          return item
        except queue.Empty:
          count += 1
          self.index = (self.index + 1) % self.pending
      raise queue.Empty
    elif self.scheduler == "fifo":
      item = self.pending.get(timeout=0)
    else:
      raise Exception("Not Implemented")

  def run(self):
    functions = self.params["functions"]
    pipeline = self.params["pipeline"]
    while self.running:
      try:
        item = self.pending.get(timeout=5)
        prefix = item.prefix
        name = pipeline[prefix]["name"]
        payload = item.payload
        if prefix < len(pipeline):
          s = payload["Records"][0]["s3"]
          input_key = s["object"]["key"]
          rparams = {**self.params, **s}
          rparams["prefix"] = prefix
          rparams["scheduler"] = True
          [input_format, output_format, bucket_format] = util.get_formats(input_key, functions[name]["file"], rparams)
          log_file = util.file_name(bucket_format)
          self.queue.put([item.job_id, log_file, item, time.time()])
        else:
          self.results.put(item)
          self.condition.acquire()
          self.condition.notify()
          self.condition.release()
      except queue.Empty:
        pass


class Parser(threading.Thread):
  def __init__(self, parser_id, scheduler, q, pending, params, client):
    super(Parser, self).__init__()
    self.handler_id = parser_id
    self.queue = q
    self.logs = []
    self.params = params
    self.pending = pending
    self.running = True
    self.log_length = 1000
    self.scheduler = scheduler
    self.client = client

  def run(self):
    s3 = util.s3(self.params)
    pipeline = self.params["pipeline"]
    while self.running:
      i = 0
      while i < len(self.logs):
        [job_id, log_file, item, start_time] = self.logs[i]
        objs = list(map(lambda o: o.key, util.get_objects(self.params["bucket"], log_file[:log_file.rindex("-")], self.params))).sort()
        if len(objs) > 0:
          self.logs = self.logs[:i] + self.logs[i + 1:]
          prefix = util.parse_file_name(objs[0])["prefix"]
          done = False
          while not done:
            try:
              obj = s3.Object(self.params["log"], log_file)
              content = obj.get()["Body"].read()
              lparams = json.loads(content.decode("utf-8"))
              for p in lparams["payloads"]:
                self.pending.put(Item(self.scheduler, obj.last_modified, prefix, job_id, p))
              done = True
            except botocore.errorfactory.NoSuchKey:
              time.sleep(1)
        elif (time.time() - start_time) > 5*60:
          prefix = item.prefix
          name = pipeline[prefix]["name"]
          util.invoke(self.client, name, self.params, payload)
        else:
          i += 1

      while len(self.logs) <= self.log_length and not self.queue.empty():
        try:
          self.logs.append(self.queue.get(timeout=0))
        except queue.Empty:
          pass


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
  def __init__(self, scheduler, params):
    self.params = dict(params)
    self.params["object"] = {}
    self.params["scheduler"] = False
    self.scheduler = scheduler
    self.q = queue.Queue()
    self.condition = threading.Condition()
    self.results = queue.Queue()
    self.client = util.setup_client("lambda", self.params)
    self.pending = PriorityQueue(self.scheduler)
    self.workers = []

  def setup(self):
    max_workers = 100
    max_parsers = 100

    for i in range(max_workers):
      self.workers.append(Worker(i, self.scheduler, self.pending, self.q, self.params, self.condition, self.results))
      self.workers[-1].start()

    for i in range(max_parsers):
      self.workers.append(Parser(i, self.scheduler, self.q, self.pending, self.params, self.client))
      self.workers[-1].start()

    # job_id = 0
    # for job_id in range(len(start_keys)):
    # start_key = start_keys[job_id]
    # m = util.parse_file_name(start_key)
    # item = Item(m["timestamp"], 0, job_id, payload(params["bucket"], start_key))
    # pending.put(item)
    return self.pending

  def wait(self, concurrency):
    while self.results.qsize() < self.params["num_output"] * concurrency:
      print("Actual", self.results.qsize(), "Expected", self.params["num_output"] * concurrency)
      self.condition.acquire()
      self.condition.wait()
      self.condition.release()

    print("Shutting down")
    for worker in self.workers:
      worker.running = False

    for worker in self.workers:
      worker.join()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())

  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
#  schedule(start_keys, params)


if __name__ == "__main__":
  main()

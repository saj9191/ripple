import argparse
from datetime import timezone
import botocore
import json
import queue
import random
import threading
import time
import util

class Item:
  def __init__(self, policy, priority, prefix, job_id, payload):
    self.priority = priority
    self.prefix = prefix
    self.payload = payload
    self.job_id = job_id
    self.policy = policy

  def __lt__(self, other):
    if self.policy == "robin":
      return self.priority < other.priority
    elif self.policy == "fifo":
      return [self.job_id, self.priority] < [other.job_id, other.priority]


class PriorityQueue:
  def __init__(self, policy):
    self.policy = policy
    if policy == "fifo":
      self.queue = queue.PriorityQueue()
    elif policy == "robin":
      self.queue = []
      self.index = 0
    else:
      raise Exception("Not Implemented")

  def put(self, item):
    if self.policy == "fifo":
      self.queue.put(item)
    elif self.policy == "robin":
      while len(self.queue) <= item.job_id:
        self.queue.append(queue.PriorityQueue())
      self.queue[item.job_id].put(item)
    else:
      raise Exception("Not Implemented")

  def get(self):
    if self.policy == "fifo":
      item = self.queue.get(timeout=0)
      return item
    elif self.policy == "robin":
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
  def __init__(self, worker_id, policy, pending_queue, running_queue, params, condition, done_queue):
    super(Worker, self).__init__()
    self.worker_id = worker_id
    self.done_queue = done_queue
    self.pending_queue = pending_queue
    self.running_queue = running_queue
    self.running = True
    self.params = dict(params)
    self.condition = condition
    self.index = 0
    self.policy = policy

  def run(self):
    functions = self.params["functions"]
    pipeline = self.params["pipeline"]
    while self.running:
      try:
        item = self.pending_queue.get()
        prefix = item.prefix
        payload = item.payload
        if prefix < len(pipeline):
          name = pipeline[prefix]["name"]
          s = payload["Records"][0]["s3"]
          input_key = s["object"]["key"]
          input_format = util.parse_file_name(input_key)
          rparams = {**self.params, **s}
          rparams["prefix"] = prefix
          rparams["scheduler"] = True
          rparams["file"] = functions[name]["file"]
          [output_format, bucket_format] = util.get_formats(input_format, rparams)
          log_file = util.file_name(bucket_format)
          self.running_queue.put([item.job_id, log_file, item, item.priority])
        else:
          self.done_queue.put(item)
      except queue.Empty:
        pass
    return


class Parser(threading.Thread):
  def __init__(self, s, parser_id, policy, running_queue, pending_queue, params, client, log_keys):
    super(Parser, self).__init__()
    self.parser_id = parser_id
    self.pending_queue = pending_queue
    self.running_queue = running_queue
    self.logs = set()
    self.log_data = {}
    self.params = params
    self.running = True
    self.log_length = 1000
    self.policy = policy
    self.client = client
    self.params["payloads"] = []
    self.s = s
    self.log_keys = log_keys

  def run(self):
    s3 = util.s3(self.params)
    pipeline = self.params["pipeline"]
    while self.running:
      found = self.log_keys.intersection(self.logs)
      not_found = self.logs.difference(self.log_keys)
      for log_file in found:
        [job_id, log_file, item, start_time] = self.log_data[log_file]
        prefix = util.parse_file_name(log_file)["prefix"]
        done = False
        while not done:
          try:
            obj = s3.Object(self.params["log"], log_file)
            content = obj.get()["Body"].read()
            lparams = json.loads(content.decode("utf-8"))
            for p in lparams["payloads"]:
              self.pending_queue.put(Item(self.policy, obj.last_modified.replace(tzinfo=timezone.utc).timestamp(), prefix, job_id, p))
            done = True
          except botocore.errorfactory.NoSuchKey:
            time.sleep(random.randint(1, 5))
        time.sleep(random.randint(1,10))

      self.logs = self.logs.difference(found)
      for log_file in not_found:
        [job_id, log_file, item, start_time] = self.log_data[log_file]
        if (time.time() - start_time) > self.params["timeout"] and self.s.run:
          now = time.time()
          self.log_data[log_file][3] = now
          prefix = item.prefix
          name = pipeline[prefix]["name"]
          print("Cannot find", name, self.params["log"], log_file)
          self.log_data[log_file][-1] = time.time()
          item.payload["continue"] = True
          util.invoke(self.client, name, self.params, item.payload)
          time.sleep(random.randint(1, 10))

      while len(self.logs) <= self.log_length and not self.running_queue.empty():
        try:
          item = self.running_queue.get(timeout=0)
          self.logs.add(item[1])
          self.log_data[item[1]] = item
        except queue.Empty:
          pass
      time.sleep(random.randint(1, 10))
    return


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
  def __init__(self, policy, params, run=True):
    self.params = dict(params)
    self.params["object"] = {}
    self.params["scheduler"] = False

    self.client = util.setup_client("lambda", self.params)
    self.condition = threading.Condition()
    self.done_queue = queue.Queue()
    self.log_keys = set()
    self.pending_queue = PriorityQueue(policy)
    self.run = run
    self.running_queue = queue.Queue()
    self.policy = policy
    self.workers = []

  def setup(self):
    max_workers = 50
    max_parsers = 50

    for worker_id in range(max_workers):
      self.workers.append(Worker(worker_id, self.policy, self.pending_queue, self.running_queue, self.params, self.condition, self.done_queue))
      self.workers[-1].start()

    for parser_id in range(max_parsers):
      self.workers.append(Parser(self, parser_id, self.policy, self.running_queue, self.pending_queue, self.params, self.client, self.log_keys))
      self.workers[-1].start()

    # job_id = 0
    # for job_id in range(len(start_keys)):
    # start_key = start_keys[job_id]
    # m = util.parse_file_name(start_key)
    # item = Item(m["timestamp"], 0, job_id, payload(params["bucket"], start_key))
    # pending.put(item)
    return self.pending_queue

  def wait(self, concurrency):
    s3 = util.s3(self.params)
    num_output = None

    while self.done_queue.qsize() == 0 or num_output is None or self.done_queue.qsize() < (num_output * concurrency):
      try:
        self.log_keys = set(list(map(lambda o: o.key, s3.Bucket(self.params["log"]).objects.all())))
      except Exception:
        pass
      time.sleep(random.randint(1, 10))
      if len(self.log_keys) > 0 and num_output is None:
        num_output = util.parse_file_name(list(self.log_keys)[0])["num_files"]

    print("Shutting down")
    for worker_id in range(len(self.workers)):
      self.workers[worker_id].running = False

    invokes = []
    for worker in self.workers:
      worker.join()
      invokes += worker.invokes
    with open("invokes", "w+") as f:
      f.write(json.dumps({"invokes": invokes}))



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

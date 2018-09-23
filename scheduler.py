import argparse
import json
import queue
import threading
import time
import util


class Item:
  def __init__(self, priority, prefix, job_id, payload):
    self.priority = priority
    self.prefix = prefix
    self.payload = payload
    self.job_id = job_id

  def __lt__(self, other):
    return self.priority < other.priority


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
  def __init__(self, worker_id, scheduler, pending, q, params, condition, client, results):
    super(Worker, self).__init__()
    self.worker_id = worker_id
    self.pending = pending
    self.queue = q
    self.running = True
    self.params = dict(params)
    self.condition = condition
    self.client = client
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
        item = self.pending.get()
        prefix = item.prefix
        payload = item.payload
        if prefix < len(pipeline):
          name = pipeline[prefix]["name"]
          util.invoke(self.client, name, self.params, payload)
          s = payload["Records"][0]["s3"]
          input_key = s["object"]["key"]
          rparams = {**self.params, **s}
          rparams["prefix"] = prefix
          rparams["scheduler"] = True
          [input_format, output_format, bucket_format] = util.get_formats(input_key, functions[name]["file"], rparams)
          log_file = util.file_name(bucket_format)
          self.queue.put([item.job_id, log_file])
        else:
          print("Worker", self.worker_id, "Found final")
          self.results.put(item)
          self.condition.acquire()
          self.condition.notify()
          self.condition.release()
      except queue.Empty:
        time.sleep(5)


class Parser(threading.Thread):
  def __init__(self, parser_id, scheduler, q, pending, params):
    super(Parser, self).__init__()
    self.handler_id = parser_id
    self.queue = q
    self.logs = []
    self.params = params
    self.pending = pending
    self.running = True
    self.log_length = 1000
    self.scheduler = scheduler

  def run(self):
    s3 = util.s3(self.params)
    while self.running:
      i = 0
      while i < len(self.logs):
        [job_id, log_file] = self.logs[i]
        if util.object_exists(self.params["log"], log_file):
          self.logs = self.logs[:i] + self.logs[i + 1:]
          prefix = util.parse_file_name(log_file)["prefix"]
          obj = s3.Object(self.params["log"], log_file)
          content = obj.get()["Body"].read()
          lparams = json.loads(content.decode("utf-8"))
          for p in lparams["payloads"]:
            self.pending.put(Item(obj.last_modified, prefix, job_id, p))
        else:
          i += 1

      while len(self.logs) <= self.log_length and not self.queue.empty():
        try:
          self.logs.append(self.queue.get(timeout=0))
        except queue.Empty:
          pass

      time.sleep(1)


def schedule(start_keys, params):
  max_workers = 100
  max_parsers = 100
  results = queue.Queue()
  q = queue.Queue()
  workers = []
  params["object"] = {}
  params["scheduler"] = False
  condition = threading.Condition()
  client = util.setup_client("lambda", params)
  scheduler = "robin"

  pending = PriorityQueue(scheduler)
  for i in range(max_workers):
    workers.append(Worker(i, scheduler, pending, q, params, condition, client, results))
    workers[-1].start()

  for i in range(max_parsers):
    workers.append(Parser(i, scheduler, q, pending, params))
    workers[-1].start()

  job_id = 0
  for job_id in range(len(start_keys)):
    start_key = start_keys[job_id]
    m = util.parse_file_name(start_key)
    item = Item(m["timestamp"], 0, job_id, {
      "Records": [{
        "s3": {
          "bucket": {
            "name": params["bucket"],
          },
          "object": {
            "key": start_key,
          },
        }
      }]
    })
    pending.put(item)

  while results.qsize() < params["num_output"] * len(start_keys):
    print("Actual", results.qsize(), "Expected", params["num_output"])
    condition.acquire()
    condition.wait()
    condition.release()

  for worker in workers:
    worker.running = False

  for worker in workers:
    worker.join()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())

  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  start_keys = [
    "0/1537682868.154564-338/1/1-1-tide.tif",
    "0/1537712854.948869-887/1/1-1-tide.tif"
  ]
  schedule(start_keys, params)


if __name__ == "__main__":
  main()

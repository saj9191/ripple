import inspect
import os
import queue
import sys
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util


class Item:
  def __init__(self, priority, prefix, job_id, deadline, payload, params):
    self.deadline = deadline
    self.job_id = job_id
    self.params = params
    self.payload = payload
    self.prefix = prefix
    self.priority = priority
    self.start_time = None
    self.__setup__()

  def __setup__(self):
    s3_payload = self.payload["Records"][0]["s3"]
    object_key = s3_payload["object"]["key"]
    input_format = util.parse_file_name(object_key)
    params = {**self.params, **s3_payload}
    if "extra_params" in s3_payload:
      params = {**params, **s3_payload["extra_params"]}

    params["prefix"] = self.prefix
    name = self.params["pipeline"][self.prefix]["name"]
    params["file"] = self.params["functions"][name]["file"]
    [output_format, bucket_format] = util.get_formats(input_format, params)
    self.output_file = util.file_name(bucket_format)

  def __lt__(self, other):
    return [self.deadline, self.priority] < [other.deadline, other.priority]


class PriorityQueue:
  def put(self, item):
    raise Exception("Not Implemented")

  def get(self):
    raise Exception("Not Implemented")


class Fifo(PriorityQueue):
  def __init__(self):
    self.queue = queue.Queue()

  def put(self, item):
    self.queue.put(item)

  def get(self):
    return self.queue.get(timeout=0)


class Robin(PriorityQueue):
  def __init__(self):
    self.queue = {}
    self.index = 0

  def put(self, item):
    if item.job_id not in self.queue:
      self.queue[item.job_id] = queue.PriorityQueue()
    self.queue[item.job_id].put(item)

  def get(self):
    jobs = list(self.queue.keys())
    num_jobs = len(jobs)
    jobs.sort()
    for i in range(num_jobs):
      index = self.index
      self.index = (self.index + 1) % num_jobs
      try:
        item = self.queue[jobs[index]].get(timeout=0)
        return item
      except queue.Empty:
        pass

    # No items found
    raise queue.Empty


class Deadline(PriorityQueue):
  def __init__(self):
    self.queue = queue.PriorityQueue()

  def put(self, item):
    self.queue.put(item)

  def get(self):
    return self.queue.get(timeout=0)

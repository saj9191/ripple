import queue


class Item:
  def __init__(self, priority, prefix, job_id, deadline, payload):
    self.priority = priority
    self.prefix = prefix
    self.payload = payload
    self.job_id = job_id
    self.deadline = deadline

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

import argparse
import boto3
import json
import math
import queue
import random
import threading
import time
import upload


class Distribution:
  def __init__(self, params):
    random.seed(0)
    self.params = params

  def generate(self):
    raise Exception("Not implemented")


class Bursty(Distribution):
  def __init__(self, params):
    Distribution.__init__(self, params)

  def generate(self):
    requests = []
    num_uniform = self.params["num_uniform"]
    cycle = num_uniform + 1
    interval = self.params["interval"]
    num_intervals = int(self.params["duration"] / (cycle * interval))
    for i in range(num_intervals):
      for j in range(num_uniform):
        requests.append(self.params["start_offset"] + (i * cycle + j) * interval)
      for j in range(self.params["max_requests"]):
        requests.append(self.params["start_offset"] + (i * cycle + num_uniform) * interval)
    return requests


class Uniform(Distribution):
  def __init__(self, params):
    Distribution.__init__(self, params)

  def generate(self):
    requests = []
    num_intervals = int(self.params["duration"] / self.params["interval"])
    for i in range(num_intervals):
      for j in range(self.params["num_interval_requests"]):
        requests.append(i * self.params["interval"] + self.params["start_offset"])

    return requests


class Diurnal(Distribution):
  def __init__(self, params):
    Distribution.__init__(self, params)

  def generate(self):
    requests = []
    cycle = self.params["interval"] + self.params["idle"]
    num_intervals = int(self.params["duration"] / cycle)
    half_interval = int(self.params["interval"] / 2)
    interval_requests = self.params["num_interval_requests"]
    increment = int(half_interval / interval_requests)

    for i in range(num_intervals):
      for j in range(0, half_interval, increment):
        num_requests = math.ceil(interval_requests * (float(j + 1) / half_interval))
        for k in range(num_requests):
          requests.append(i * cycle + self.params["start_offset"] + j)
          requests.append(i * cycle + self.params["interval"] + self.params["start_offset"] - j)

    requests.sort()
    return requests


class Request(threading.Thread):
  def __init__(self, thread_id, date_time, request_queue, response_queue, params):
    super(Request, self).__init__()
    self.date_time = date_time
    self.error = None
    self.start_time = time.time()
    self.params = params
    self.request_queue = request_queue
    self.response_queue = response_queue
    self.thread_id = thread_id

  def run(self):
    while self.error is None and not self.request_queue.empty():
      try:
        print("Thread", self.thread_id, "Number of requests remaining", self.request_queue.qsize())
        [file_name, request_date_time] = self.request_queue.get(timeout=0)
        request_delta = request_date_time - self.date_time
        assert(request_delta >= 0)
        now = time.time()
        time_delta = now - self.start_time
        sleep = max(0, request_delta - time_delta)
        time.sleep(sleep)
        print("Thread", self.thread_id, "Wakeup", request_date_time)
        self.response_queue.put(upload.upload(self.params["bucket"], file_name, self.params["input_bucket"]))
      except queue.Empty as e:
        pass


def create_requests(params):
  if params["distribution"] == "uniform":
    distribution = Uniform(params)
  elif params["distribution"] == "diurnal":
    distribution = Diurnal(params)
  elif params["distribution"] == "bursty":
    distribution = Bursty(params)
  else:
    raise Exception("Not implemented")

  requests = distribution.generate()
  return requests


def run(params, m, distribution, output_folder):
  s3 = boto3.resource("s3")
  file_names = list(map(lambda o: o.key, s3.Bucket(params["input_bucket"]).objects.filter(Prefix=params["input_prefix"])))
  file_names = list(filter(lambda k: not k.endswith("/"), file_names))
  num_files = len(file_names)
  requests = create_requests(params)

  threads = []
  request_queue = queue.Queue()
  response_queue = queue.Queue()
  if distribution:
    print(requests)
    return
  for i in range(len(requests)):
    request_queue.put([file_names[i % num_files], requests[i]])

  for i in range(params["num_threads"]):
    threads.append(Request(i, 0, request_queue, response_queue, params))
    threads[-1].start()

  while request_queue.qsize() > 0:
    if m.error is not None:
      for thread in threads:
        thread.error = m.error
      raise Exception("Error", m.error)
    time.sleep(10)

  for thread in threads:
    thread.join()

  if output_folder:
    responses = list(response_queue.queue)
    with open(output_folder + "/upload_stats", "w+") as f:
      f.write(json.dumps({"stats": responses}))

class DummyMaster:
  def __init__(self):
    self.error = None

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing simulation distribution parameters")
  parser.add_argument("--distribution", action="store_true", help="Just print the distribution timestamps")
  parser.add_argument("--output_folder", type=str, help="Folder to store timestamp results")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params, DummyMaster(), args.distribution, args.output_folder)


if __name__ == "__main__":
  main()

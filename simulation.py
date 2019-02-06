import argparse
import boto3
import json
import math
import numpy as np
import queue
import random
from scipy import special
import setup
import scheduler
import threading
import time
import upload
import util


class Distribution:
  def __init__(self, params):
    random.seed(0)
    self.params = params

  def generate(self):
    raise Exception("Not implemented")


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
  def __init__(self, thread_id, date_time, request_queue, params):
    super(Request, self).__init__()
    self.date_time = date_time
    self.error = None
    self.start_time = time.time()
    self.params = params
    self.request_queue = request_queue
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
        upload.upload(self.params["bucket"], file_name, self.params["input_bucket"])
      except queue.Empty as e:
        pass


def create_requests(params):
  if params["distribution"] == "uniform":
    distribution = Uniform(params)
  elif params["distribution"] == "diurnal":
    distribution = Diurnal(params)
  else:
    raise Exception("Not implemented")

  requests = distribution.generate()
  return requests


def run(params, m):
  s3 = boto3.resource("s3")
  file_names = list(map(lambda o: o.key, s3.Bucket(params["input_bucket"]).objects.filter(Prefix=params["input_prefix"])))
  file_names = list(filter(lambda k: not k.endswith("/"), file_names))
  num_files = len(file_names)
  requests = create_requests(params)

  threads = []
  request_queue = queue.Queue()
  for i in range(len(requests)):
    request_queue.put([file_names[i % num_files], requests[i]])

  for i in range(params["num_threads"]):
    threads.append(Request(i, 0, request_queue, params))
    threads[-1].start()

  while request_queue.qsize() > 0:
    if m.error is not None:
      for thread in threads:
        thread.error = m.error
      raise Exception("Error", m.error)
    time.sleep(10)

  for thread in threads:
    thread.join()


class DummyMaster:
  def __init__(self):
    self.error = None


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing simulation distribution parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params, DummyMaster())


if __name__ == "__main__":
  main()









def old():
  random.seed(0)
  max_concurrency = 200
  if distribution in ["uniform", "zipfian"]:
    interval = 60  # Request every 60 seconds
    #  num_intervals = 10
  else:
    interval = 7 * 60
    #  num_intervals = int((time_range + interval) / interval)

  if distribution == "uniform":
    num_requests = 1
    for i in range(0, time_range + 1, interval):
      requests += [i] * num_requests
  elif distribution == "zipfian":
    # https://stackoverflowcom/questions/43601074/zipf-distribution-how-do-i-measure-zipf-distribution-using-python-numpy.
    x = np.arange(1, 61)
    a = 2.
    y = x**(-a) / special.zetac(a) * max_concurrency
    y = list(map(lambda i: int(i), list(y)))
    random.shuffle(y)
    for i in range(len(y)):
      requests += [i * interval] * y[i]
  elif distribution == "bursty":
    interval = 1200  # Every 20 minutes
    for i in range(interval, time_range + interval, interval):
      requests += [i] * 10
      #requests += [i] * 100#max_concurrency
  elif distribution == "concurrent":
    for i in range(args.concurrency):
      requests.append(args.delay * i)



DATE_INDEX = 6
BINS = 60 * 60


lock = threading.Lock()




def parse_csv(file_name):
  dates = []
  counts = {}
  f = open(file_name)
  lines = f.readlines()[1:]
  for line in lines:
    dt = line.split(",")[DATE_INDEX]
    dt = dt[1:-2]
    parts = dt.split(" ")
    if len(parts) != 2:
      continue
    date = parser.parse(dt)
    s = "{0:d}-{1:02d}-{2:02d}".format(date.year, date.month, date.day)
    if "2017-09-25" <= s and s <= "2017-10-21":
      dates.append(date)
    if s not in counts:
      counts[s] = 0
    counts[s] += 1

  #keys = list(counts.keys())
  #keys.sort()
  #for key in keys:
  #  print(key, counts[key])
  #return counts
  return dates
    #if parts[0] not in datetimes:
    #  datetimes[parts[0]] = []
    #datetimes[parts[0]].append(date.hour * 60 * 60 + date.minute * 60 + date.second)

  #for date in datetimes:
  #  if len(datetimes[date]) == num_requests:
  #    return datetimes[date]


class Second(threading.Thread):
  def __init__(self, s, params):
    super(Second, self).__init__()
    self.s = s
    self.params = params

  def run(self):
    self.s.wait(self.params["num_output"])


def deadline(params):
  folder = params["folder"]
  with open("results/{0:s}/params".format(folder), "w+") as f:
    f.write(json.dumps(params, indent=4, sort_keys=True))

  setup.setup(params)
  benchmark.process_params(params)
  benchmark.process_iteration_params(params, 1)

  threads = []
  client = None

  params["scheduler"] = True
  deadline_params = dict(params)
  deadline_params["stats"] = False
  m = {
    "prefix": "0",
    "timestamp": deadline_params["now"],
    "nonce": deadline_params["nonce"],
    "bin": 1,
    "file_id": 1,
    "continue": False,
    "suffix": "tide",
    "num_files": 1,
    "ext": deadline_params["ext"]
  }
  deadline_params["key"] = util.file_name(m)
  [upload_timestamp, upload_duration] = benchmark.upload_input(deadline_params, 0)
  sleep = 30

  q = queue.Queue()
  q.put(params["input_name"])
  threads.append(Request(1, sleep, q, dict(params), client, wait=True))
  threads[0].start()
  s = scheduler.Scheduler("fifo", deadline_params, run=False)
  q = s.setup()
  payload = scheduler.payload(deadline_params["bucket"], deadline_params["key"])
  payload["continue"] = True
  q.put(scheduler.Item("fifo", upload_duration, 0, 0, payload))
  second = Second(s, deadline_params)
  second.start()
  threads[-1].join()
  print("Restarting first job")
  s.run = True
  second.join()
#  s.wait(deadline_params["num_output"])


def run1(args, params):
  session = boto3.Session(
           aws_access_key_id=params["access_key"],
           aws_secret_access_key=params["secret_key"],
           region_name=params["region"]
  )
  s3 = session.resource("s3")
  if "sample_bucket" in params:
    file_names = list(map(lambda o: o.key, s3.Bucket(params["sample_bucket"]).objects.all()))
  else:
    file_names = [params["input_name"]]

  folder = params["folder"]
  with open("results/{0:s}/params".format(folder), "w+") as f:
    f.write(json.dumps(params, indent=4, sort_keys=True))

  task = "run"
  if params["model"] == "lambda" and task == "run":
    setup.setup(params)

  requests = create_request_distribution(args.distribution, args)
  client = None
  if params["model"] == "ec2":
    benchmark.process_params(params)
    benchmark.process_iteration_params(params, 1)
    create_stats = benchmark.create_instance(params)
    instance = create_stats["instance"]
    ec2 = create_stats["ec2"]
    initiate_stats = benchmark.initiate_instance(ec2, instance, params)
    client = initiate_stats["client"]
    benchmark.setup_instance(client, params)

  i = 0
  threads = []
  schedule = "fifo"
  s = scheduler.Scheduler(schedule, params)
  scheduler_queue = s.setup()
  request_queue = queue.Queue()
  num_requests = len(requests)

  for i in range(len(requests)):
    request_queue.put(file_names[i % len(file_names)])

  num_threads = 300
  for i in range(num_threads):
    threads.append(Request(i, 0, request_queue, params, client, scheduler_queue, task))
    threads[-1].start()

  s.wait(num_requests)
  for thread in threads:
    thread.join()
  #  threads = []
#    if len(requests) > 0:
#      temp_time = current_time
#      current_time = requests[0]
#      time.sleep(current_time - temp_time)

  #  stats = benchmark.parse_logs(thread.params, 0, 0, 0)
  #  dir_path = "results/{0:s}/{1:f}-{2:d}".format(thread.params["folder"], thread.params["now"], thread.params["nonce"])
  #  os.makedirs(dir_path)
  #  with open("{0:s}/stats".format(dir_path), "w+") as f:
  #    f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))
  #  benchmark.clear_buckets(thread.params)

  print("Processed {0:d} requests".format(num_requests))
  #if params["model"] == "ec2":
  #  benchmark.terminate_instance(instance, client, params)



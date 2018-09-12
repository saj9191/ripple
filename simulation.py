import argparse
import benchmark
import boto3
from dateutil import parser
import json
import numpy as np
import os
import plot
import random
from scipy import special
import setup
import threading
import time
import util


DATE_INDEX = 6
BINS = 60 * 60


class Request(threading.Thread):
  def __init__(self, thread_id, time, file_name, params):
    super(Request, self).__init__()
    self.time = time
    self.file_name = file_name
    self.params = dict(params)
    self.thread_id = thread_id
    self.duration = 0
    self.upload_duration = 0
    self.failed_attempts = 0
    self.alive = True

  def run(self):
    time.sleep(self.time)
    self.params["input_name"] = self.file_name
    print("Thread {0:d}: Processing file {1:s}".format(self.thread_id, self.file_name))
    [upload_duration, duration, failed_attempts] = benchmark.run(self.params, self.thread_id)
    self.upload_duration = upload_duration
    self.duration = duration
    self.failed_attempts = failed_attempts
    print("Thread {0:d}: Done in {1:f}".format(self.thread_id, duration))
    msg = "Thread {0:d}: Upload Duration {1:f}. Duration {2:f}. Failed Attempts {3:f}"
    msg = msg.format(self.thread_id, self.upload_duration, self.duration, self.failed_attempts)
    print(msg)
    stats = benchmark.parse_logs(self.params, self.params["now"] * 1000, self.upload_duration, self.duration)
    dir_path = "results/{0:s}/{1:f}-{2:d}".format(self.params["folder"], self.params["now"], self.params["nonce"])
    os.makedirs(dir_path)
    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))
    benchmark.clear_buckets(self.params)
    self.alive = False


def parse_csv(file_name, num_requests):
  datetimes = {}
  f = open(file_name)
  lines = f.readlines()[1:]
  for line in lines:
    dt = line.split(",")[DATE_INDEX]
    dt = dt[1:-2]
    parts = dt.split(" ")
    if len(parts) != 2:
      continue
    date = parser.parse(dt)

    if parts[0] not in datetimes:
      datetimes[parts[0]] = []
    datetimes[parts[0]].append(date.hour * 60 * 60 + date.minute * 60 + date.second)

  for date in datetimes:
    if len(datetimes[date]) == num_requests:
      return datetimes[date]


def launch_threads(requests, file_names, params):
  requests.sort()

  threads = []
  for i in range(len(requests)):
    thread = Request(i, requests[i], file_names[i % len(file_names)], params)
    thread.start()
    threads.append(thread)

  for thread in threads:
    thread.join()

  now = time.time()
  folder = "results/{0:s}/{1:f}".format(params["folder"], now)
  if not os.path.isdir(folder):
    os.makedirs(folder)

  params["timestamp"] = now
  plot.plot(threads, params["pipeline"], params)

  with open("{0:s}/long_benchmark.csv".format(folder), "w+") as f:
    for thread in threads:
      msg = "{0:d},{1:f},{2:f},{3:f},{4:d}\n".format(thread.thread_id, thread.upload_duration, thread.duration, thread.failed_attempts, thread.time)
      f.write(msg)


def create_request_distribution(distribution):
  random.seed(0)
  max_concurrency = 1000
  time_range = 60 * 60  # 1 hour
  requests = []
  interval = 60  # Request every 60 seconds
  num_intervals = int((time_range + interval) / interval)

  if distribution == "uniform":
    num_requests = 1
    for i in range(0, time_range + 1, interval):
      requests += [i] * num_requests
  elif distribution == "zipfian":
    # https://stackoverflowcom/questions/43601074/zipf-distribution-how-do-i-measure-zipf-distribution-using-python-numpy.
    x = np.arange(0, num_intervals)
    a = 2.
    y = x**(-a) / special.zetac(a) * max_concurrency
    y = random.shuffle(list(map(lambda i: int(i), list(y))))
    for i in range(len(y)):
      requests += [i * interval] * y[i]
  elif distribution == "exponential":
    for i in range(num_intervals):
      num_requests = 2 ** i
      requests += [i * interval] * num_requests

  return requests


def run(args, params):
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
  with open("{0:s}/params".format(folder), "w+") as f:
    f.write(json.dumps(params, indent=4, sort_keys=True))

  setup.setup(params)
  requests = create_request_distribution(args.distribution)

  i = 0
  time_delta = 100
  current_time = 0
  threads = []
  while len(requests) > 0:
    current_requests = list(filter(lambda r: r - time < time_delta, requests))
    requests = list(filter(lambda r: r - time >= time_delta, requests))
    for request in current_requests:
      threads.append(Request(i, request - current_time, file_names[i % len(file_names)], params))
      i += 1

    threads = list(filter(lambda t: t.alive, threads))
    if len(requests) > 0:
      temp_time = current_time
      current_time = requests[0]
      time.sleep(current_time - temp_time)

  while len(threads) > 0:
    threads = list(filter(lambda t: t.alive, threads))
    time.sleep(10)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--distribution", type=str, required=True, help="Distribution to use")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["setup"] = False
  params["iterations"] = 1
  run(args, params)


if __name__ == "__main__":
  create_request_distribution("zipfian")

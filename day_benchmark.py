import argparse
import benchmark
import boto3
from dateutil import parser
import json
import os
import plot
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

  def run(self):
    [access_key, secret_key] = util.get_credentials("default")
    self.params["input_name"] = self.file_name
    self.params["access_key"] = access_key
    self.params["secret_key"] = secret_key
    print("Thread {0:d}: Processing file {1:s}".format(self.thread_id, self.file_name))
    [upload_duration, duration, failed_attempts] = benchmark.run(self.params, self.thread_id)
    self.upload_duration = upload_duration
    self.duration = duration
    self.failed_attempts = failed_attempts
    print("Thread {0:d}: Done in {1:f}".format(self.thread_id, duration))


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

  folder = "concurrency{0:d}".format(len(threads))
  if not os.path.isdir(folder):
    os.makedirs(folder)

  with open("{0:s}/{1:f}".format(folder, time.time()), "w+") as f:
    for thread in threads:
      f.write("{0:f}-{1:d}\n".format(thread.params["now"], thread.params["nonce"]))

  for thread in threads:
    msg = "Thread {0:d}: Upload Duration {1:f}. Duration {2:f}. Failed Attempts {3:f}"
    msg = msg.format(thread.thread_id, thread.upload_duration, thread.duration, thread.failed_attempts)
    print(msg)
    print("Thread {0:d}: Timestamp {1:f}. Nonce {2:d}".format(thread.thread_id, thread.params["now"], thread.params["nonce"]))
    stats = benchmark.parse_logs(thread.params, thread.params["now"] * 1000, thread.upload_duration, thread.duration)
    dir_path = "results/{0:f}-{1:d}".format(thread.params["now"], thread.params["nonce"])
    os.makedirs(dir_path)
    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

    deps = benchmark.create_dependency_chain(stats[1:-1], 1)
    with open("{0:s}/deps".format(dir_path), "w+") as f:
      f.write(json.dumps(deps, indent=4, sort_keys=True, default=benchmark.serialize))

  plot.plot(threads, params["pipeline"], params)

  with open("long_benchmark.csv", "w+") as f:
    for thread in threads:
      msg = "{0:d},{1:f},{2:f},{3:f},{4:d}\n".format(thread.thread_id, thread.upload_duration, thread.duration, thread.failed_attempts, thread.time)
      f.write(msg)


def run(args, params):
  session = boto3.Session(
           aws_access_key_id=params["access_key"],
           aws_secret_access_key=params["secret_key"],
           region_name=params["region"]
  )
  s3 = session.resource("s3")
  file_names = list(map(lambda o: o.key, s3.Bucket("shjoyner-sample-input").objects.all()))

  setup.setup(params)
  for i in range(0, 5):
    requests = []
    num_requests = max(i * 100, 1)
    for j in range(num_requests):
      requests.append(i)
    done = False
    while not done:
      try:
        launch_threads(requests, file_names, params)
        done = True
      except Exception as e:
        print("Error. Retry.", e)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--num_requests", type=int, required=True, help="Number of requests to send throughout the day")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["setup"] = False
  params["stats"] = False
  params["iterations"] = 1
  params["sample_input"] = True
  run(args, params)


if __name__ == "__main__":
  main()

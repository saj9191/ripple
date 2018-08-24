import argparse
import benchmark
import boto3
from dateutil import parser
import json
import os
import plot
import setup
import shutil
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
    msg = "Thread {0:d}: Upload Duration {1:f}. Duration {2:f}. Failed Attempts {3:f}"
    msg = msg.format(self.thread_id, self.upload_duration, self.duration, self.failed_attempts)
    print(msg)
    stats = benchmark.parse_logs(self.params, self.params["now"] * 1000, self.upload_duration, self.duration)
    dir_path = "results/{0:f}-{1:d}".format(self.params["now"], self.params["nonce"])
    os.makedirs(dir_path)
    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

    match_stage = -1
    pipeline = self.params["pipeline"]
    for i in range(len(pipeline)):
      if self.params["functions"][pipeline[i]["name"]]["file"] == "match":
        match_stage = i + 1

    if match_stage != -1:
      prefix = "{0:d}/{1:f}-{2:d}".format(match_stage, self.params["now"], self.params["nonce"])
      objects = util.get_objects(self.params["bucket"], prefix)
      for obj in objects:
        content = obj.get()["Body"].read().decode("utf-8")
        print("Thread {0:d}: {1:s}\n".format(self.thread_id, content))
    benchmark.clear_buckets(self.params)


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
  folder = "results/concurrency{0:d}/{1:f}".format(len(threads), now)
  if not os.path.isdir(folder):
    os.makedirs(folder)

  shutil.copyfile(params["params_name"], "{0:s}/params.json".format(folder))

  with open("{0:s}/files".format(folder), "w+") as f:
    for thread in threads:
      file = "{0:f}-{1:d}".format(thread.params["now"], thread.params["nonce"])
      f.write("{0:s}\n".format(file))
      input_file = "results/{0:s}".format(file)
      shutil.copytree(input_file, "{0:s}/{1:s}".format(folder, file))
      shutil.rmtree(input_file)

  #plot.plot(threads, params["pipeline"], params)

  with open("{0:s}/long_benchmark.csv".format(folder), "w+") as f:
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
  for i in range(0, 2):
    requests = []
    num_requests = max(i * 50, 1)
    for j in range(num_requests):
      requests.append(i)
    done = False
    while not done:
      launch_threads(requests, file_names, params)
      done = True


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
  params["params_name"] = args.parameters
  run(args, params)


if __name__ == "__main__":
  main()

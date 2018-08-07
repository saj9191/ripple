import argparse
import benchmark
from dateutil import parser
import json
import queue
import threading
import time
import util


DATE_INDEX = 6
BINS = 60 * 60


class Request(threading.Thread):
  def __init__(self, thread_id, time, file_name, q, params):
    super(Request, self).__init__()
    self.time = time
    self.file_name = file_name
    self.params = dict(params)
    self.queue = q
    self.thread_id = thread_id

  def run(self):
  s3 = boto3.resource("s3")
  program_bucket = s3.Bucket(params["program_bucket"])

  with open("/tmp/ssw_test", "wb") as f:
    program_bucket.download_fileobj("ssw_test", f)

    print("Thread {0:d}: Sleeping for {1:d} seconds".format(self.thread_id, self.time))
    time.sleep(self.time)
    [access_key, secret_key] = util.get_credentials("default")
    self.params["input_name"] = self.file_name
    self.params["access_key"] = access_key
    self.params["secret_key"] = secret_key
    print("Thread {0:d}: Processing file {1:s}".format(self.thread_id, self.file_name))
    benchmark.run(self.params)
    self.queue.put((self.params["now"], self.params["nonce"]))
    print("Thread {0:d}: Done".format(self.thread_id))


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
  q = queue.Queue()

  threads = []
  for i in range(len(requests)):
    thread = Request(i, requests[i], file_names[i % len(file_names)], q, params)
    thread.start()
    threads.append(thread)

  for thread in threads:
    thread.join()

  with not q.empty:
    print(q.get())


def run(args, params):
  requests = parse_csv(args.file, args.num_requests)
  file_names = ["a", "b", "c"]
  requests = [0, 100, 200]
  launch_threads(requests, file_names, params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--file", type=str, required=True, help="CSV Chorus access log")
  parser.add_argument("--num_requests", type=int, required=True, help="Number of requests to send throughout the day")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["plot"] = False
  params["setup"] = False
  params["iterations"] = 1
  params["sample_input"] = True
  run(args, params)


if __name__ == "__main__":
  main()

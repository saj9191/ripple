import argparse
import boto3
import constants
import datetime
from enum import Enum
import json
import os
import paramiko
import plot
import random
import re
import setup
import subprocess
import time
import util

MASS = re.compile("Z\s+([0-9\.]+)\s+([0-9\.]+)")
MEMORY_PARAMETERS = json.loads(open("json/memory.json").read())
CHECKS = json.loads(open("json/checks.json").read())
REPORT = re.compile(".*RequestId:\s([^\s]*)\tDuration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Size:\s([0-9]+)\sMB.*Used:\s([0-9]+)\sMB.*")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")
STAT_FIELDS = ["cost", "max_duration", "memory_used"]
INVOKED_REGEX = re.compile("TIMESTAMP [0-9\.]+ NONCE [0-9]+ REQUEST ID (.*) INVOKED BY REQUEST ID (.*)")
REQUEST_REGEX = re.compile("TIMESTAMP [0-9\.]+ NONCE [0-9]+ FILE ([0-9]+) REQUEST ID (.*)")
WRITE_REGEX = re.compile("TIMESTAMP [0-9\.]+ NONCE [0-9]+ WRITE REQUEST ID (.*) FILE NAME (.*)")
READ_REGEX = re.compile("TIMESTAMP [0-9\.]+ NONCE [0-9]+ READ REQUEST ID (.*) FILE NAME (.*)")


#############################
#         COMMON            #
#############################


class BenchmarkException(Exception):
  pass


def get_count(obj):
  content = obj.get()["Body"].read().decode("utf8")
  return content.count("S\t")


def check_sort(s3, params):
  keys = []
  bucket_name = "maccoss-human-merge-spectra"
  bucket = s3.Bucket(bucket_name)
  now = "{0:f}".format(params["now"])
  for obj in bucket.objects.all():
    if now in obj.key:
      keys.append(obj.key)

  keys.sort(key=lambda k: int(k.split("-")[2]))
  mass = 0
  for key in keys:
    obj = s3.Object(bucket_name, key)
    content = obj.get()["Body"].read().decode("utf-8")
    spectra = constants.SPECTRA_START.split(content)
    spectra = list(filter(lambda p: len(p) > 0, spectra))
    for spectrum in spectra:
      lines = spectrum.split("\n")
      m = list(filter(lambda line: constants.MASS.match(line), lines))
      if len(m) == 0:
        print(spectrum)
      assert(len(m) > 0)
      new_mass = float(constants.MASS.match(m[0]).group(2))
      assert(mass <= new_mass)
      mass = new_mass


def check_output(params):
  s3 = setup_connection("s3", params)

  prefix = "tide-search-{0:f}-{1:d}".format(params["now"], params["nonce"])
  bucket_name = params["pipeline"][-2]["output_bucket"]
  print("Checking output from bucket", bucket_name)
  bucket = s3.Bucket(bucket_name)
  for obj in bucket.objects.all():
    if obj.key.startswith(prefix):
      content = obj.get()["Body"].read().decode("utf-8")
      num_lines = len(content.split("\n"))
      print("key", obj.key, "num_lines", num_lines, flush=True)

  bucket_name = params["pipeline"][-1]["output_bucket"]
  bucket = s3.Bucket(bucket_name)
  for obj in bucket.objects.all():
    token = "{0:f}-{1:d}".format(params["now"], params["nonce"])
    if token in obj.key and "target" in obj.key:
      content = obj.get()["Body"].read().decode("utf-8")

      lines = content.split("\n")[1:]
      lines = list(filter(lambda line: len(line.strip()) > 0, lines))
      qvalues = list(map(lambda line: float(line.split("\t")[7]), lines))
      count = len(list(filter(lambda qvalue: qvalue <= CHECKS["qvalue"], qvalues)))
      print("key", obj.key, "qvalues", count, flush=True)


def print_run_information():
  git_output = subprocess.check_output("git log --oneline | head -n 1", shell=True).decode("utf-8").strip()
  print("Current Git commit", git_output, "\n", flush=True)


def process_params(params):
  _, ext = os.path.splitext(params["input_name"])
  params["ext"] = ext[1:]
  params["input"] = params["input_name"]
  params["input_bucket"] = params["pipeline"][0]["input_bucket"]
  params["output_bucket"] = params["pipeline"][-1]["output_bucket"]
  for i in range(len(params["pipeline"])):
    params["pipeline"][i]["num_bins"] = params["num_bins"]
    params["pipeline"][i]["num_buckets"] = params["num_buckets"]


def process_iteration_params(params, iteration):
  now = time.time()
  params["now"] = now
  params["nonce"] = random.randint(1, 1000)
  m = {
    "prefix": "spectra",
    "timestamp": params["now"],
    "nonce": params["nonce"],
    "bin": 1,
    "file-id": 1,
    "last": True,
    "ext": params["ext"]
  }
  params["key"] = util.file_name(m)


def upload_input(params):
  bucket_name = params["input_bucket"]
  s3 = setup_connection("s3", params)
  key = params["key"]

  start = time.time()
  print("Uploading {0:s} to s3://{1:s}".format(params["input"], bucket_name), flush=True)
  s3.Object(bucket_name, key).put(Body=open("data/{0:s}".format(params["input"]), 'rb'))
  end = time.time()

  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print(key, "last modified", timestamp, flush=True)
  seconds = end - start
  milliseconds = seconds * 1000

  return int(timestamp), milliseconds


def load_stats(upload_duration):
  return {
    "name": "load",
    "billed_duration": [upload_duration],
    "max_duration": upload_duration,
    "memory_used": 0,
    "cost": 0,
    "messages": [],
  }


def benchmark(i, params):
  done = False
  while not done:
    try:
      if params["model"] == "lambda":
        results = lambda_benchmark(params)
      elif params["model"] == "ec2":
        results = ec2_benchmark(params)
      elif params["model"] == "coordinator":
        results = coordinator_benchmark(params)

      done = True
    except BenchmarkException as e:
      print("Error during iteration {0:d}".format(i), e, flush=True)
      clear_buckets(params)

  return results


def serialize(obj):
  return obj.json()


class Request:
  def __init__(self, name, request_id):
    self.name = name
    self.request_id = request_id
    self.parent_key = None
    self.duration = 0
    self.file_id = None
    self.children = set()

  def json(self):
    s = {
      "name": self.name,
      "request_id": self.request_id,
      "parent_key": self.parent_key,
      "duration": self.duration,
      "file_id": self.file_id,
      "children": list(self.children),
    }
    return s

  def __repr__(self):
    return json.dumps(self.json())


def process_request(message, dependencies, request_to_file, name, layer):
  m = REQUEST_REGEX.match(message)
  if m is not None:
    file_id = int(m.group(1))
    key = "{0:d}:{1:d}".format(layer, file_id)
    request_id = m.group(2)
    if request_id not in dependencies:
      request = Request(name, request_id)
      dependencies[key] = request
      dependencies[key].file_id = file_id
      request_to_file[request_id] = file_id
      if name == "map_blast":
        dependencies[key].parent_key = "{0:d}:1".format(layer - 1)
      elif name == "combine-pivot-files" and file_id == 1:
        dependencies[key].children.add("{0:d}:1".format(layer + 1))


def process_read(message, file_writes, dependencies, request_to_file, name, layer):
  m = READ_REGEX.match(message)
  if m is not None:
    request_id = m.group(1)
    file_id = request_to_file[request_id]
    file_name = m.group(2)
    parent_id = file_writes[file_name]
    key = "{0:d}:{1:d}".format(layer, file_id)
    if name == "sort-blast-chunk":
      file_id = 1
    else:
      file_id = request_to_file[parent_id]
    parent_key = "{0:d}:{1:d}".format(layer - 1, file_id)
    # print(name, message)
    # print("parent key", parent_key, "key", key)
    # print("num children", len(dependencies[parent_key].children))
    # print(dependencies[parent_key].name, "->", dependencies[key].name)
    dependencies[key].parent_key = parent_key
    dependencies[parent_key].children.add(key)
    assert(dependencies[key].parent_key is not None)


def process_write(message, file_writes, dependencies, request_to_file, name):
  m = WRITE_REGEX.match(message)
  if m is not None:
    request_id = m.group(1)
    file_name = m.group(2)
    request_to_file[request_id] = util.parse_file_name(file_name)["file-id"]
    # print("RID", request_id, "name", file_name, "file_id", request_to_file[request_id])
    file_writes[file_name] = request_id


def process_invoke(message, dependencies, request_to_file, name, layer):
  m = INVOKED_REGEX.match(message)
  if m is not None:
    request_id = m.group(1)
    parent_id = m.group(2)

    file_id = request_to_file[request_id]
    key = "{0:d}:{1:d}".format(layer, file_id)
    if name == "sort-blast-chunk":
      file_id = 1
    else:
      file_id = request_to_file[parent_id]
    parent_key = "{0:d}:{1:d}".format(layer - 1, file_id)
    # print(name, message)
    # print("parent key", parent_key, "key", key)
    # print(dependencies[parent_key].name, "->", dependencies[key].name)
    # print("num children", len(dependencies[parent_key].children))
    dependencies[key].parent_key = parent_key
    dependencies[parent_key].children.add(key)
    assert(dependencies[key].parent_key is not None)


def process_report(message, dependencies, request_to_file, name, layer):
  m = REPORT.match(message)
  if m is not None:
    print(name, message)
    request_id = m.group(1)
    file_id = request_to_file[request_id]
    duration = int(m.group(3))
    key = "{0:d}:{1:d}".format(layer, file_id)
    dependencies[key].duration += duration


def create_dependency_chain(stats, iterations):
  stats = stats
  dependencies = {}
  file_writes = {}
  request_to_file = {}
  for layer in range(len(stats)):
    for i in range(len(stats[layer])):
      stat = stats[layer][i]
      name = stat["name"]
      messages = stat["messages"]
      for message in messages:
        process_request(message, dependencies, request_to_file, name, layer)

  for layer in range(len(stats)):
    for i in range(len(stats[layer])):
      stat = stats[layer][i]
      name = stat["name"]
      messages = stat["messages"]
      for message in messages:
        process_report(message, dependencies, request_to_file, name, layer)
        process_invoke(message, dependencies, request_to_file, name, layer)
        process_write(message, file_writes, dependencies, request_to_file, name)
        process_read(message, file_writes, dependencies, request_to_file, name, layer)

  for key in dependencies:
    dependencies[key].duration = float(dependencies[key].duration) / iterations

  return dependencies


def run(params):
  print_run_information()
  process_params(params)
  iterations = params["iterations"]

  setup.setup(params)
  pipeline = [{"name": "load"}] + params["pipeline"]
  stats = list(map(lambda s: [], pipeline))

  for i in range(iterations):
    process_iteration_params(params, i)
    results = benchmark(i, params)
    for j in range(len(results)):
      stats[j].append(results[j])
#    if params["check_output"]:
#      check_output(params)

    #clear_buckets(params)
    print("--------------------------\n", flush=True)

  print("END RESULTS ({0:d} ITERATIONS)".format(iterations), flush=True)

  f = open("results/stats-{0:f}-{1:d}".format(params["now"], params["nonce"]), "w+")
  f.write(json.dumps({"stats": stats}, indent=2, sort_keys=True))
  f.close()

  dependencies = create_dependency_chain(stats[1:], iterations)
  for key in dependencies:
    dependencies[key].duration = float(dependencies[key].duration) / iterations
  dep_file = "results/dep-{0:f}-{1:d}".format(params["now"], params["nonce"])
  f = open(dep_file, "w+")
  f.write(json.dumps(dependencies, indent=2, sort_keys=True, default=serialize))
  f.close()

  dependencies = json.loads(open(dep_file).read())
  plot.plot(dependencies, pipeline[1:], iterations, params)


def calculate_total_stats(stats):
  total_stats = {}
  total_stats["billed_duration"] = list(map(lambda d: 0, stats[0]["billed_duration"]))

  for i in range(len(total_stats["billed_duration"])):
    for stat in stats:
      total_stats["billed_duration"][i] += stat["billed_duration"][i]

  for field in STAT_FIELDS:
    total_stats[field] = 0

  for stat in stats:
    for field in STAT_FIELDS:
      total_stats[field] += stat[field]

  return total_stats


def calculate_average_results(stats, iterations):
  total_stats = calculate_total_stats(stats)
  average_stats = {}

  average_stats["billed_duration"] = list(map(lambda d: 0, total_stats["billed_duration"]))
  for i in range(len(total_stats["billed_duration"])):
    average_stats["billed_duration"][i] = float(total_stats["billed_duration"][i]) / iterations

  for field in STAT_FIELDS:
    average_stats[field] = float(total_stats[field]) / iterations

  return average_stats


def print_stats(stats):
  print("Total Cost", stats["cost"], flush=True)
  for i in range(len(stats["billed_duration"])):
    print("Runtime", i, stats["billed_duration"][i] / 1000, "seconds", flush=True)
  print("Total Runtime", stats["max_duration"] / 1000, "seconds", flush=True)
  print("Total Billed Duration", sum(stats["billed_duration"]) / 1000, "seconds", flush=True)
  print("Total Memory Used", stats["memory_used"], "MB", flush=True)


def setup_connection(service, params):
  session = boto3.Session(
    aws_access_key_id=params["access_key"],
    aws_secret_access_key=params["secret_key"],
    region_name=params["region"]
  )
  return session.resource(service)


def clear_buckets(params):
  s3 = setup_connection("s3", params)
  id = "{0:f}-{1:d}".format(params["now"], params["nonce"])

  def delete_objects(bucket_name):
    bucket = s3.Bucket(bucket_name)
    count = 0
    for obj in bucket.objects.all():
      if id in obj.key:
        obj.delete()
        count += 1
    print("Deleted", count, bucket_name, "objects")

  for function in params["pipeline"]:
    for bucket_name in setup.function_buckets(function):
      delete_objects(bucket_name)

  delete_objects(params["pipeline"][-1]["output_bucket"])

#############################
#       COORDINATOR         #
#############################


class CoordinatorStage(Enum):
  LOAD = 0
  CREATE = 1
  INITIATE = 2
  SETUP = 3
  DOWNLOAD = 4
  SPLIT = 5
  COMBINE = 6
  PERCOLATOR = 7
  ANALYZE = 8
  UPLOAD = 9
  TERMINATE = 10
  TOTAL = 11


SPLIT_REGEX = re.compile("SPLIT\sDURATION\s*([0-9\.]+)")
COMBINE_REGEX = re.compile("COMBINE\sDURATION\s*([0-9\.]+)")
PERCOLATOR_REGEX = re.compile("PERCOLATOR\sDURATION\s*([0-9\.]+)")


def run_coordinator(client, params):
  key = params["key"]
  batch_size = params["lambda"]["split_spectra"]["batch_size"]
  chunk_size = params["lambda"]["split_spectra"]["chunk_size"]
  prefix = params["bucket_prefix"]
  print("Running coordinator", flush=True)
  cmd = "python3 coordinator.py --file {0:s} --batch_size {1:d} --chunk_size {2:d} --bucket_prefix {3:s}".format(key, batch_size, chunk_size, prefix)
  print(cmd)
  stdout = cexec(client, cmd, True)
  print("WHAT")
  print(stdout)

  m = SPLIT_REGEX.search(stdout)
  if m is None:
    print(stdout, flush=True)
    raise BenchmarkException("No split")
  split_results = calculate_results(float(m.group(1)), MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])

  m = COMBINE_REGEX.search(stdout)
  if m is None:
    print(stdout)
    raise BenchmarkException("No combine")
  combine_results = calculate_results(float(m.group(1)), MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])

  m = PERCOLATOR_REGEX.search(stdout)
  if m is None:
    print(stdout)
    raise BenchmarkException("No percolator")
  percolator_results = calculate_results(float(m.group(1)), MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  return [split_results, combine_results, percolator_results]


def setup_coordinator_instance(client, params):
  print("Setting up EC2 Coordinator instance.")
  start_time = time.time()
  sftp = client.open_sftp()

  items = []
  if not params["coordinator"]["use_ami"]:
    cexec(client, "sudo yum update -y")
    cexec(client, "cd /etc/yum.repos.d; sudo wget http://s3tools.org/repo/RHEL_6/s3tools.repo")
    cexec(client, "sudo yum -y install s3cmd")
    cexec(client, "sudo yum -y install python34")
    cexec(client, "curl -O https://bootstrap.pypa.io/get-pip.py")
    cexec(client, "sudo python3 get-pip.py --user")
    cexec(client, "echo 'export PATH=/home/ec2-user/.local/bin:$PATH' >> ~/.bashrc")
    cexec(client, "source ~/.bashrc")
    cexec(client, "pip install awsebcli --upgrade --user")
    cexec(client, "sudo python3 -m pip install argparse")
    cexec(client, "sudo python3 -m pip install boto3")
    cexec(client, "sudo update-alternatives --set python /usr/bin/python2.6")
    items.append("crux")
    items.append("constants.py")
    items.append("spectra.py")
    items.append("split.py")
    items.append("util.py")
    items.append("header.mzML")

  cexec(client, "echo -e '{0:s}\n{1:s}\n{2:s}\n\n' | aws configure".format(params["access_key"], params["secret_key"], params["region"]))
  cexec(client, "echo -e '{0:s}\n{1:s}\n\n\n\n\nY\ny\n' | s3cmd --configure".format(params["access_key"], params["secret_key"]))
  items.append("coordinator.py")

  for item in items:
    sftp.put(item, item)

  if not params["coordinator"]["use_ami"]:
    cexec(client, "sudo chmod +x crux")

  sftp.close()
  end_time = time.time()

  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  results["client"] = client
  return results


def coordinator_benchmark(params):
  stats = []

  [upload_timestamp, upload_duration] = upload_input(params)
  stats.append(load_stats(upload_duration))

  create_stats = create_instance(params)
  stats.append(create_stats)

  instance = create_stats["instance"]
  ec2 = create_stats["ec2"]
  initiate_stats = initiate_instance(ec2, instance, params)
  stats.append(initiate_stats)

  client = initiate_stats["client"]
  stats.append(setup_coordinator_instance(client, params))
  stats.append(download_input(client, params))
  stats += run_coordinator(client, params)

  lclient = util.setup_client("logs", params)
  stats.append(parse_analyze_logs(lclient, params))
  stats.append(upload_results(client, params))
  stats.append(terminate_instance(instance, client, params))

  total_stats = calculate_total_stats(stats)
  print("END RESULTS")
  print_stats(total_stats)
  stats.append(total_stats)

  return stats


#############################
#         LAMBDA            #
#############################

class NonMergeLambdaStage(Enum):
  LOAD = 0
  SPLIT = 1
  ANALYZE = 2
  COMBINE = 3
  PERCOLATOR = 4
  TOTAL = 5


class MergeLambdaStage(Enum):
  LOAD = 0
  SPLIT = 1
  SORT = 2
  MERGE = 3
  ANALYZE = 4
  COMBINE = 5
  PERCOLATOR = 6
  TOTAL = 7


def check_objects(client, bucket_name, prefix, count, timeout, params):
  done = False
  suffix = ""
  if count > 1:
    suffix = "s"

  # There's apparently a stupid bug where str(timestamp) has more significant
  # digits than "{0:f}:.format(timestmap)
  # eg. spectra-1530978873.960075-1-0-58670073.txt 1530978873.9600754
  ts = "{0:f}".format(params["now"])

  start = datetime.datetime.now()
  s3 = setup_connection("s3", params)
  bucket = s3.Bucket(bucket_name)
  while not done:
    c = 0
    now = time.time()
    for obj in bucket.objects.all():
      # print(obj.key, ts, obj.key.startswith(prefix), ts in obj.key)
      if obj.key.startswith(prefix) and ts in obj.key:
        c += 1
    # print("count", c, count)
    done = (c == count)
    end = datetime.datetime.now()
    now = end.strftime("%H:%M:%S")
    if not done:
      print("{0:s}: Waiting for {1:s} function{2:s}. Timeout is {3:d} seconds".format(now, prefix, suffix, timeout), flush=True)
      if (end - start).total_seconds() > timeout:
        raise BenchmarkException("Could not find bucket {0:s} prefix {1:s}".format(bucket_name, prefix))
      time.sleep(30)
    else:
      print("{0:s}: Found {1:s} function{2:s}".format(now, prefix, suffix), flush=True)


def wait_for_completion(start_time, params):
  client = util.setup_client("s3", params)

  # Give ourselves time as we need to wait for each part of the pipeline
  last = params["pipeline"][-1]
  timeout = 6 * 60
  bucket_name = last["output_bucket"]
  check_objects(client, bucket_name, "ssw", params["num_bins"], timeout, params)
  print("")
  time.sleep(10)  # Wait a little to make sure percolator logs are on the server


def fetch(client, log_name, timestamp, nonce, filter_pattern, extra_args={}):
  log_events = []
  next_token = None
  args = {
    "logGroupName": "/aws/lambda/{0:s}".format(log_name),
    "startTime": int(timestamp),
  }
  args = {**args, **extra_args}

  more = True
  while more:
    args["filterPattern"] = "TIMESTAMP {0:f} NONCE {1:d}".format(timestamp / 1000, nonce)
    if next_token:
      args["nextToken"] = next_token
    response = client.filter_log_events(**args)
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

    log_events += response["events"]
    if "nextToken" not in response:
      more = False
    else:
      next_token = response["nextToken"]

  messages = list(map(lambda l: l["message"], log_events))
  args["filterPattern"] = filter_pattern

  les = {}
  for event in log_events:
    if event["logStreamName"] not in les:
      les[event["logStreamName"]] = 0
    if "FILE" in event["message"]:
      les[event["logStreamName"]] += 1

  events = []
  for name in les.keys():
    count = les[name]
    args["logStreamNames"] = [name]
    args["limit"] = count
    response = client.filter_log_events(**args)
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    es = response["events"]
    messages += list(map(lambda e: e["message"], es))
    events += es

  return [events, messages]


def fetch_events(client, log_name, start_time, nonce, filter_pattern, extra_args={}):
  events = fetch(client, log_name, start_time * 1000, nonce, filter_pattern, extra_args)
  if len(events) == 0:
    raise BenchmarkException("Could not find any events")
  return events


def calculate_cost(duration, memory_size):
  # Cost per 100ms
  millisecond_cost = MEMORY_PARAMETERS["lambda"][str(memory_size)]
  return int(duration / 100) * millisecond_cost


def file_count(bucket_name, params):
  s3 = setup_connection("s3", params)
  bucket = s3.Bucket(bucket_name)
  now = "{0:f}".format(params["now"])
  count = 0
  for obj in bucket.objects.all():
    if now in obj.key:
      count += 1
  return count


def parse_mult_logs(client, params, lparams, num_lambdas=None):
  [events, messages] = fetch_events(client, lparams["name"], params["now"], params["nonce"], "REPORT RequestId")
  max_billed_duration = 0
  min_billed_duration = 5*60*1000
  total_billed_duration = 0
  total_memory_used = 0
  min_timestamp = events[0]["timestamp"]
  max_timestamp = events[0]["timestamp"]
  min_memory = 4000
  max_memory = 0
  durations = []

  for event in events:
    min_timestamp = min(min_timestamp, event["timestamp"])
    max_timestamp = max(max_timestamp, event["timestamp"])
    m = REPORT.match(event["message"])
    duration = int(m.group(3))
    memory_used = int(m.group(5))
    min_memory = min(memory_used, min_memory)
    max_memory = max(memory_used, max_memory)
    min_billed_duration = min(min_billed_duration, duration)
    max_billed_duration = max(max_billed_duration, duration)
    total_billed_duration += duration
    total_memory_used += memory_used
    durations.append(duration)

  cost = calculate_cost(total_billed_duration, lparams["memory_size"])

  print(lparams["name"])
  print("num events", len(events))
  print("Min Timestamp", min_timestamp)
  print("Max Timestamp", max_timestamp)
  print("Min Memory", min_memory)
  print("Max Memory", max_memory)
  print("Min Billed Duration", min_billed_duration, "milliseconds")
  print("Max Billed Duration", max_billed_duration, "milliseconds")
  print("Total Billed Duration", total_billed_duration, "milliseconds")
  print("Cost", cost)
  print("", flush=True)

  return {
    "name": lparams["name"],
    "billed_duration": durations,
    "max_duration": max_billed_duration,
    "memory_used": total_memory_used,
    "cost": cost,
    "messages": messages
  }


def parse_analyze_logs(client, params):
  return parse_mult_logs(client, params, "analyze_spectra")


def parse_logs(params, upload_timestamp, upload_duration):
  client = util.setup_client("logs", params)

  stats = []
  stats.append(load_stats(upload_duration))
  for i in range(len(params["pipeline"])):
    step = params["pipeline"][i]
    stats.append(parse_mult_logs(client, params, step))

  return stats


def lambda_benchmark(params):
  [upload_timestamp, upload_duration] = upload_input(params)
  wait_for_completion(upload_timestamp, params)
  return parse_logs(params, upload_timestamp, upload_duration)


#############################
#           EC2             #
#############################


class NoSortEc2Stage(Enum):
  LOAD = 0
  CREATE = 1
  INITIATE = 2
  SETUP = 3
  DOWNLOAD = 4
  TIDE = 5
  PERCOLATOR = 6
  UPLOAD = 7
  TERMINATE = 8
  TOTAL = 9


class SortEc2Stage(Enum):
  LOAD = 0
  CREATE = 1
  INITIATE = 2
  SETUP = 3
  DOWNLOAD = 4
  SORT = 5
  TIDE = 6
  PERCOLATOR = 7
  UPLOAD = 8
  TERMINATE = 9
  TOTAL = 10


def calculate_results(duration, cost):
  milliseconds = duration * 1000
  return {
    "billed_duration": milliseconds,
    "cost": (float(duration) * cost) / 60,
    "max_duration": milliseconds,
    "memory_used": 0
  }


def create_instance(params):
  print("Creating instance")
  ec2 = setup_connection("ec2", params)
  ami = params["ec2"]["default_ami"]
  if params["model"] == "ec2" and params["ec2"]["use_ami"]:
    ami = params["ec2"]["ami"]
  elif params["model"] == "coordinator" and params["coordinator"]["use_ami"]:
    ami = params["coordinator"]["ami"]

  start_time = time.time()
  instances = ec2.create_instances(
    ImageId=ami,
    InstanceType=params["ec2"]["type"],
    KeyName=params["ec2"]["key"],
    MinCount=1,
    MaxCount=1,
    NetworkInterfaces=[{
      "SubnetId": params["ec2"]["subnet"],
      "DeviceIndex": 0,
      "Groups": [params["ec2"]["security"]]
    }],
    TagSpecifications=[{
      "ResourceType": "instance",
      "Tags": [{
        "Key": "Name",
        "Value": "maccoss-benchmark-{0:f}".format(params["now"])
      }]
    }]
  )
  assert(len(instances) == 1)
  instance = instances[0]
  print("Waiting for instance to initiate.")
  instance.wait_until_running()
  end_time = time.time()
  duration = end_time - start_time

  results = calculate_results(duration, 0)
  results["instance"] = instance
  results["ec2"] = ec2
  return results


def cexec(client, command, error=False):
  (stdin, stdout, stderr) = client.exec_command(command)
  stdout.channel.recv_exit_status()
  if error:
    print(stderr.read())
  return stdout.read().decode("utf-8")


def connect(instance, params):
  client = paramiko.SSHClient()
  pem = params["ec2"]["key"] + ".pem"
  key = paramiko.RSAKey.from_private_key_file(pem)
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  connected = False
  while not connected:
    try:
      client.connect(
        instance.public_ip_address,
        username="ec2-user",
        pkey=key
      )
      connected = True
    except paramiko.ssh_exception.NoValidConnectionsError:
      time.sleep(1)
  return client


def initiate_instance(ec2, instance, params):
  print("Connecting to EC2 instance.")
  start_time = time.time()

  if params["ec2"]["wait_for_tests"]:
    print("Wait for status checks.")
    instance_status = list(ec2.meta.client.describe_instance_status(InstanceIds=[instance.id])["InstanceStatuses"])[0]
    while instance_status["InstanceStatus"]["Details"][0]["Status"] == "initializing":
      instance_status = list(ec2.meta.client.describe_instance_status(InstanceIds=[instance.id])["InstanceStatuses"])[0]
      time.sleep(1)
    assert(instance_status["InstanceStatus"]["Details"][0]["Status"] == "passed")

  instance.reload()
  client = connect(instance, params)
  end_time = time.time()

  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  results["client"] = client
  return results


def setup_instance(client, params):
  print("Setting up EC2 instance.")
  start_time = time.time()
  sftp = client.open_sftp()

  items = []
  if not params["ec2"]["use_ami"]:
    cexec(client, "sudo yum update -y")
    cexec(client, "cd /etc/yum.repos.d; sudo wget http://s3tools.org/repo/RHEL_6/s3tools.repo")
    cexec(client, "sudo yum -y install s3cmd")
    cexec(client, "sudo update-alternatives --set python /usr/bin/python2.6")
    cexec(client, "sudo yum -y install python-pip")
    cexec(client, "sudo pip install argparse")
    cexec(client, "echo -e '{0:s}\n{1:s}\n\n\n\n\nY\ny\n' | s3cmd --configure".format(params["access_key"], params["secret_key"]))
    items.append("crux")
    items.append("HUMAN.fasta.20170123")
    items.append("sort.py")
    items.append("constants.py")

    index_dir = "HUMAN.fasta.20170123.index"
    sftp.mkdir(index_dir)
    for item in os.listdir(index_dir):
      path = "{0:s}/{1:s}".format(index_dir, item)
      sftp.put(path, path)

  for item in items:
    sftp.put(item, item)

  if not params["ec2"]["use_ami"]:
    cexec(client, "sudo chmod +x crux")

  sftp.close()
  end_time = time.time()

  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  results["client"] = client
  return results


def download_input(client, params):
  start_time = time.time()
  cexec(client, "s3cmd get s3://{0:s}/{1:s} {1:s}".format(params["input_bucket"], params["key"]))
  end_time = time.time()
  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  return results


def sort_spectra(client, params):
  if params["ext"] != "ms2":
    raise Exception("sort_spectra: Not implemented for ext", params["ext"])
  start_time = time.time()
  cexec(client, "python sort.py --file {0:s}".format(params["key"]))
  params["key"] = "sorted_{0:s}".format(params["key"])
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def run_analyze(client, params):
  print("Running Tide", flush=True)
  arguments = [
    "--num-threads", str(params["lambda"]["analyze_spectra"]["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", "tide-output",
    "--overwrite", "T",
  ]
  start_time = time.time()
  command = "sudo ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(params["key"], " ".join(arguments))
  cexec(client, command)
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def run_percolator(client, params):
  print("Running Percolator", flush=True)
  arguments = [
    "--subset-max-train", str(params["lambda"]["percolator"]["max_train"]),
    "--quick-validation", "T",
    "--overwrite", "T",
  ]

  start_time = time.time()
  cexec(client, "sudo ./crux percolator {0:s} {1:s}".format("tide-output/tide-search.txt", " ".join(arguments)))
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def upload_results(client, params):
  print("Uploading files to s3")
  bucket_name = params["output_bucket"]
  start_time = time.time()
  for item in ["peptides", "psms"]:
    input_file = "percolator.target.{0:s}.txt".format(item)
    output_file = "percolator.target.{0:s}.{1:f}.{2:d}.txt".format(item, params["now"], params["nonce"])
    cexec(client, "s3cmd put crux-output/{0:s} s3://{1:s}/{2:s}".format(input_file, bucket_name, output_file))

  input_file = "tide-output/tide-search.txt"
  output_file = util.file_name(params["now"], params["nonce"], 1, 1, 1, "txt")
  cexec(client, "s3cmd put {0:s} s3://{1:s}/{2:s}".format(input_file, bucket_name, output_file))

  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def terminate_instance(instance, client, params):
  start_time = time.time()
  client.close()
  instance.terminate()
  instance.wait_until_terminated()
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, 0)


def ec2_benchmark(params):
  print("EC2 benchmark")
  [upload_timestamp, upload_duration] = upload_input(params)

  stats = []
  stats.append(load_stats(upload_duration))

  create_stats = create_instance(params)
  stats.append(create_stats)

  instance = create_stats["instance"]
  ec2 = create_stats["ec2"]
  initiate_stats = initiate_instance(ec2, instance, params)
  stats.append(initiate_stats)

  client = initiate_stats["client"]
  stats.append(setup_instance(client, params))
  stats.append(download_input(client, params))
  if params["sort"] == "yes":
    stats.append(sort_spectra(client, params))

  stats.append(run_analyze(client, params))
  stats.append(run_percolator(client, params))
  stats.append(upload_results(client, params))
  stats.append(terminate_instance(instance, client, params))

  total_stats = calculate_total_stats(stats)
  print("END RESULTS")
  print_stats(total_stats)
  stats.append(total_stats)

  return stats

#############################
#           MAIN            #
#############################


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  print(params)
  run(params)


if __name__ == "__main__":
  main()

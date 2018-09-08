import argparse
import boto3
import datetime
import json
import os
import paramiko
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
INVOKED_REGEX = re.compile("([0-9\.]+) - .* STEP ([0-9]+) BIN ([0-9]+) FILE ([0-9]+) REQUEST ID (.*) TOKEN ([0-9]+) INVOKED BY TOKEN ([0-9]+)")
REQUEST_REGEX = re.compile("([0-9\.]+) - .* STEP ([0-9]+) BIN ([0-9]+) FILE ([0-9]+) REQUEST ID (.*) TOKEN ([0-9]+)$")
WRITE_REGEX = re.compile("([0-9\.]+) - .* STEP ([0-9]+) BIN ([0-9]+) WRITE REQUEST ID (.*) TOKEN ([0-9]+) FILE NAME (.*)")
READ_REGEX = re.compile("([0-9\.]+) - .* STEP ([0-9]+) BIN ([0-9]+) READ REQUEST ID (.*) TOKEN ([0-9]+) FILE NAME (.*)")
DURATION_REGEX = re.compile("([0-9\.]+) - .* STEP ([0-9]+) BIN [0-9]+ FILE ([0-9]+) REQUEST ID (.*) TOKEN ([0-9]+) DURATION ([0-9]+)")
COUNT_REGEX = re.compile("STEP ([0-9]+) TOKEN ([0-9]+) READ COUNT ([0-9]+) WRITE COUNT ([0-9]+) LIST COUNT ([0-9]+) BYTE COUNT ([0-9]+)")

#############################
#         COMMON            #
#############################


class BenchmarkException(Exception):
  pass


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
  params["input_bucket"] = params["bucket"]
  params["output_bucket"] = params["bucket"]
  for i in range(len(params["pipeline"])):
    for p in ["num_bins", "num_buckets", "timeout"]:
      if p in params:
        params["pipeline"][i][p] = params[p]
        params["pipeline"][i][p] = params[p]
        params["pipeline"][i][p] = params[p]


def process_iteration_params(params, iteration):
  now = time.time()
  if params["model"] == "lambda" and util.is_set(params, "trigger"):
    input_format = util.parse_file_name(params["input_name"])
    params["now"] = input_format["timestamp"]
    params["nonce"] = input_format["nonce"]
  else:
    params["now"] = now
    params["nonce"] = random.randint(1, 1000)

  m = {
    "prefix": "0",
    "timestamp": params["now"],
    "nonce": params["nonce"],
    "bin": 1,
    "file_id": 1,
    "suffix": "tide",
    "last": True,
    "ext": params["ext"]
  }
  if params["model"] == "ec2":
    params["key"] = params["input_name"]
  else:
    if util.is_set(params, "trigger"):
      params["key"] = params["input_name"]
    else:
      params["key"] = util.file_name(m)


def upload_input(p, thread_id=0):
  bucket_name = p["input_bucket"]
  s3 = setup_connection("s3", p)
  key = p["key"]

  start = time.time()
  if util.is_set(p, "trigger"):
    client = util.setup_client("lambda", p)
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": p["bucket"],
          },
          "object": {
            "key": p["input_name"],
          }
        }
      }]
    }
    client.invoke(
      FunctionName=p["pipeline"][0]["name"],
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )

  elif util.is_set(p, "sample_input"):
    print("Thread {0:d}: Moving {1:s} to s3://{2:s}".format(thread_id, p["input_name"], bucket_name), flush=True)
    s3.Object(bucket_name, key).copy_from(CopySource={"Bucket": p["sample_bucket"], "Key": p["input_name"]}, StorageClass=p["storage_class"])
  else:
    print("Uploading {0:s} to s3://{1:s}".format(p["input"], bucket_name), flush=True)
    s3.Object(bucket_name, key).put(Body=open("data/{0:s}".format(p["input"]), 'rb'), StorageClass=p["storage_class"])
  end = time.time()

  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print("Thread {0:d}: Handling key {1:s}. Last modified {2:f}".format(thread_id, key, timestamp), flush=True)
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


def benchmark(i, params, thread_id=0):
  failed = False
  if params["model"] == "lambda":
    [failed, results] = lambda_benchmark(params, thread_id)
  elif params["model"] == "ec2":
    results = ec2_benchmark(params)

  return results + [failed]


def serialize(obj):
  return obj.json()


class Request:
  def __init__(self, name, timestamp, token, request_id, file_id):
    self.name = name
    self.request_id = request_id
    self.token = token
    self.timestamp = timestamp
    self.parent_key = ""
    self.duration = 0
    self.file_id = file_id
    self.children = set()
    self.read_count = 0
    self.write_count = 0
    self.list_count = 0

  def json(self):
    s = {
      "name": self.name,
      "request_id": self.request_id,
      "parent_key": self.parent_key,
      "duration": self.duration,
      "timestamp": self.timestamp,
      "file_id": self.file_id,
      "children": list(self.children),
    }
    return s

  def __repr__(self):
    return json.dumps(self.json())


def process_request(message, dependencies, token_to_file, name, start_timestamp):
  m = INVOKED_REGEX.match(message)
  if m is None:
    n = DURATION_REGEX.match(message)
    if n is None:
      m = REQUEST_REGEX.match(message)

  if m is not None:
    timestamp = float(m.group(1))
    layer = int(m.group(2))
    file_id = int(m.group(4))
    request_id = m.group(5)
    token = m.group(6)
    key = "{0:d}:{1:s}".format(layer, token)
    file_id = int(m.group(2))
    if key not in dependencies:
      offset = timestamp - start_timestamp
      if offset < 0:
        print("process_request", layer, offset)
      request = Request(name, offset, key, request_id, file_id)
      dependencies[key] = request


def process_read(message, file_writes, dependencies, token_to_file, name):
  m = READ_REGEX.match(message)
  if m is not None:
    layer = int(m.group(2))
    token = m.group(5)
    key = "{0:d}:{1:s}".format(layer, token)
    file_name = m.group(6).replace("/tmp/", "")

    if layer != 0 and dependencies[key].parent_key == "":
      if file_name in file_writes:
        parent_key = file_writes[file_name]
      else:
        print("Can't find parent for {0:s}".format(key))
        parent_keys = list(filter(lambda k: k.startswith("{0:d}:".format(layer-1)), dependencies.keys()))
        parent_key = parent_keys[0]

    if layer != 0 and dependencies[key].parent_key == "":
      dependencies[key].parent_key = parent_key
      dependencies[parent_key].children.add(key)
      assert(dependencies[key].parent_key is not None)


def process_write(message, file_writes, dependencies, token_to_file, name):
  m = WRITE_REGEX.match(message)
  if m is not None:
    layer = int(m.group(2))
    token = m.group(5)
    key = "{0:d}:{1:s}".format(layer, token)
    file_name = m.group(6).replace("/tmp/", "")
    file_writes[file_name] = key


def process_invoke(message, dependencies, token_to_file, name):
  m = INVOKED_REGEX.match(message)
  if m is not None:
    layer = int(m.group(2))
    token = int(m.group(6))
    key = "{0:d}:{1:d}".format(layer, token)
    parent_token = int(m.group(7))
    parent_key = "{0:d}:{1:d}".format(layer - 1, parent_token)
    if parent_key in dependencies:
      dependencies[key].parent_key = parent_key
      dependencies[parent_key].children.add(key)
    else:
      print("process_invoke", "can't find parent", key)


def process_report(message, dependencies, token_to_file, name, layers_to_cost, layers_to_count, params):
  m = DURATION_REGEX.match(message)
  if m is not None:
    layer = int(m.group(2))
    token = int(m.group(5))
    key = "{0:d}:{1:d}".format(layer, token)
    duration = int(m.group(6))
    dependencies[key].duration = duration
    memory_size = str(params["functions"][name]["memory_size"])
    cost = MEMORY_PARAMETERS["lambda"][memory_size] * int((duration + 99) / 100)
    layers_to_cost[layer] += cost
    layers_to_count[layer] += 1


def process_counts(message, dependencies, name, layers_to_cost):
  m = COUNT_REGEX.match(message)
  if m is not None:
    layer = int(m.group(1)) - 1
    read_count = int(m.group(3))
    write_count = int(m.group(4))
    list_count = int(m.group(5))
    byte_count = int(m.group(6))
    layers_to_cost[layer] += ((write_count + list_count) / 1000) * 0.005
    layers_to_cost[layer] += (read_count / 1000) * 0.0004
    layers_to_cost[layer] += (float(byte_count) / (1024 * 1024 * 1024)) * 0.0007


def create_dependency_chain(stats, iterations, params):
  stats = stats
  file_writes = {}
  token_to_file = {}
  layers_to_cost = {}
  layers_to_count = {}
  dependencies = {}

  stats = stats[0]
  stats = list(filter(lambda s: s["name"] not in ["load", "total"], stats))

  if params["model"] == "ec2":
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(params["bucket"])
    content_length = 0
    for obj in bucket.objects.all():
      o = s3.Object(params["bucket"], obj.key)
      content_length += o.content_length
    layers_to_cost[0] = (content_length / (1024 * 1024 * 1024)) * 0.023
    layers_to_cost[0] += (1.0 / 1000) * 0.0005
    layers_to_cost[0] += (1.0 / 1000) * 0.0004
    layers_to_count[0] = 1
  else:
    for i in range(len(stats)):
      layers_to_cost[i] = 0
      layers_to_count[i] = 0
      if "content_length" in stats[i]:
        layers_to_cost[i] = (stats[i]["content_length"] / (1024 * 1024 * 1024)) * 0.023

    start_message = list(filter(lambda m: REQUEST_REGEX.match(m) is not None, stats[1]["messages"]))[0]
    start_timestamp = float(REQUEST_REGEX.match(start_message).group(1))

    for layer in range(len(stats)):
      stat = stats[layer]
      name = stat["name"]
      messages = stat["messages"]
      for message in messages:
        process_request(message, dependencies, token_to_file, name, start_timestamp)

    for layer in range(len(stats)):
      stat = stats[layer]
      name = stat["name"]
      messages = stat["messages"]
      for message in messages:
        process_invoke(message, dependencies, token_to_file, name)
        process_write(message, file_writes, dependencies, token_to_file, name)
        process_report(message, dependencies, token_to_file, name, layers_to_cost, layers_to_count, params)

    for layer in range(len(stats)):
      stat = stats[layer]
      name = stat["name"]
      messages = stat["messages"]
      for message in messages:
        process_read(message, file_writes, dependencies, token_to_file, name)
        process_counts(message, dependencies, name, layers_to_cost)

  dependencies["layers_to_cost"] = layers_to_cost
  dependencies["layers_to_count"] = layers_to_count
  return dependencies


def run(params, thread_id):
  if params["setup"]:
    print_run_information()
  process_params(params)

  if params["model"] == "lambda" and params["setup"]:
    setup.setup(params)

  total_upload_duration = 0.0
  total_duration = 0.0
  total_failed_attempts = 0.0
  iterations = params["iterations"]

  for i in range(iterations):
    stats = None
    process_iteration_params(params, i)
    if params["stats"]:
      [results, upload_duration, duration, failed] = benchmark(i, params, thread_id)
      if stats is None:
        stats = list(map(lambda i: [], range(len(results))))
      for j in range(len(results)):
        stats[j].append(results[j])
    else:
      [upload_duration, duration, failed] = benchmark(i, params, thread_id)
    total_upload_duration += upload_duration
    total_duration += duration
    total_failed_attempts += (1 if failed else 0)

    if params["stats"]:
      if util.is_set(params, "trigger"):
        params["now"] = time.time()
      dir_path = "results/{0:s}/{1:f}-{2:d}".format(params["ec2"]["application"], params["now"], params["nonce"])
      os.makedirs(dir_path)
      with open("{0:s}/stats".format(dir_path), "w+") as f:
        f.write(json.dumps({"stats": stats, "failed": failed}, indent=4, sort_keys=True))

#    if params["model"] == "lambda":
#      clear_buckets(params)

  avg_upload_duration = total_upload_duration / iterations
  avg_duration = total_duration / iterations
  avg_failed_attempts = total_failed_attempts / iterations
  print("ITERATIONS + FAILED ATTEMPTS", total_failed_attempts + iterations)
  return [avg_upload_duration, avg_duration, avg_failed_attempts]


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
  num_steps = len(params["pipeline"]) + 1
  bucket = s3.Bucket(params["bucket"])
  log_bucket = s3.Bucket(params["log"]) if "log" in params else None
  for i in range(num_steps):
    prefix = "{0:d}/{1:f}-{2:d}/".format(i, params["now"], params["nonce"])
    done = False
    while not done:
      try:
        if i != 0 or not util.is_set(params, "trigger"):
          bucket.objects.filter(Prefix=prefix).delete()
        if log_bucket:
          log_bucket.objects.filter(Prefix=prefix).delete()
        done = True
      except Exception as e:
        pass

#############################
#         LAMBDA            #
#############################


def find_current_stage(bucket_name, params):
  current_stage = 0
  for i in range(len(params["pipeline"])):
    prefix = "{0:d}/{1:f}-{2:d}".format(i, params["now"], params["nonce"])
    objects = util.get_objects(bucket_name, prefix)
    if len(objects) > 0:
      current_stage = i
  return current_stage


def check_objects(client, bucket_name, prefix, count, timeout, params, thread_id):
  done = False

  # There's apparently a stupid bug where str(timestamp) has more significant
  # digits than "{0:f}:.format(timestmap)
  # eg. spectra-1530978873.960075-1-0-58670073.txt 1530978873.9600754
  token = "{0:f}-{1:d}".format(params["now"], params["nonce"])
  prefix = "{0:s}{1:s}".format(prefix, token)
  print(params["input_name"], "Waiting for {0:s}".format(prefix))
  start = datetime.datetime.now()
  while not done:
    now = time.time()
    found = set()
    objects = util.get_objects(bucket_name, prefix)
    done = (len(objects) >= count)
    end = datetime.datetime.now()
    if not done:
      if (end - start).total_seconds() > timeout:
        expected = set(range(1, count + 1))
        current_stage = find_current_stage(bucket_name, params)
        print("Thread {0:d}: Last stage with output files is {1:d}".format(thread_id, current_stage))
        print("Could not find", expected.difference(found))
        return True
      time.sleep(30)
    else:
      now = end.strftime("%H:%M:%S")
      print("{0:s}: Thread {1:d}. Found {2:s}".format(now, thread_id, prefix), flush=True)
  return False


def wait_for_completion(start_time, params, thread_id):
  client = util.setup_client("s3", params)

  # Give ourselves time as we need to wait for each part of the pipeline
  prefix = "{0:d}/".format(len(params["pipeline"]))
  timeout = 120 * len(params["pipeline"])
  failed = check_objects(client, params["bucket"], prefix, params["num_output"], timeout, params, thread_id)
  time.sleep(10)  # Wait a little to make sure percolator logs are on the server
  return failed


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


def parse_logs(params, upload_timestamp, upload_duration, total_duration):
  print("Parsing logs")
  stats = []
  stats.append(load_stats(upload_duration))
  s3 = util.s3(params)
  log_bucket = s3.Bucket(params["log"])
  data_bucket = s3.Bucket(params["bucket"])
  messages = []

  count = 0
  for i in range(len(params["pipeline"])):
    done = False
    while not done:
      try:
        messages = []
        prefix = "{0:d}/{1:f}-{2:d}".format(i + 1, params["now"], params["nonce"])
        for obj in log_bucket.objects.filter(Prefix=prefix):
          o = s3.Object(params["log"], obj.key)
          messages.append(o.get()["Body"].read().decode("utf-8"))
          count += 1
          if count % 1000 == 0:
            print("Processed", count)
        done = True
      except Exception as e:
        print(log_bucket, data_bucket)
        print(e)
        done = False

    step = params["pipeline"][i]
    stats.append({
      "name": step["name"],
      "messages": messages
    })

  stats.append({
    "name": "total",
    "duration": total_duration,
    "messages": [],
  })
  return stats


def lambda_benchmark(params, thread_id):
  [upload_timestamp, upload_duration] = upload_input(params, thread_id)
  start_time = time.time()
  failed = wait_for_completion(upload_timestamp, params, thread_id)
  end_time = time.time()
  total_duration = end_time - start_time
  results = [upload_duration, total_duration]

  print("WTF1", params["stats"])
  if params["stats"]:
    print("WTF")
    stats = parse_logs(params, upload_timestamp, upload_duration, total_duration)
    results = [stats] + results
    print("result length", len(results))
  return [failed, results]


#############################
#           EC2             #
#############################


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

  start_time = time.time()
  print("Creating")
  instances = ec2.create_instances(
    BlockDeviceMappings=[{
      "DeviceName": "/dev/sda1",
      "Ebs": {
          "VolumeSize": 50,
      }
    }],
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
  results["name"] = "create"
  return results


def cexec(client, command, error=False):
  (stdin, stdout, stderr) = client.exec_command(command)
  stdout.channel.recv_exit_status()
  print(stderr.read().decode("utf-8"))
  return stdout.read().decode("utf-8")


def connect(instance, params):
  client = paramiko.SSHClient()
  pem = params["ec2"]["key"] + ".pem"
  print(pem)
  key = paramiko.RSAKey.from_private_key_file(pem)
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  time.sleep(10)
  connected = False
  while not connected:
    try:
      client.connect(
        instance.public_ip_address,
        username="ubuntu",
        pkey=key,
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
  results["name"] = "initiate"
  return results


def setup_instance(client, p):
  print("Setting up EC2 instance.")
  start_time = time.time()
  sftp = client.open_sftp()
  items = ["formats/iterator.py", "formats/mzML.py", "ec2_script.py", "util.py"]
  if p["ec2"]["application"] == "ssw":
    program = "ssw_test"
  elif p["ec2"]["application"] == "methyl":
    program = "output"
  else:
    program = "crux"

  print("program", program)
  if not p["ec2"]["use_ami"]:
    cexec(client, "sudo apt-get update -y")
    time.sleep(3)
    cexec(client, "sudo apt-get update -y")
    time.sleep(3)
    cexec(client, "sudo apt-get update -y")
    time.sleep(3)
    cexec(client, "sudo apt install python3-pip -y")
    cexec(client, "pip3 install boto3")
    items.append(program)
    cexec(client, "mkdir ~/.aws")
    cexec(client, "touch ~/.aws/credentials")
    cmd = 'echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/.aws/credentials'.format(p["access_key"], p["secret_key"])
    cexec(client, cmd)

  for item in items:
    sftp.put(item, item.split("/")[-1])

  if not p["ec2"]["use_ami"]:
    cexec(client, "chmod u+x {0:s}".format(program))

  sftp.close()
  end_time = time.time()

  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][p["ec2"]["type"]])
  results["name"] = "setup"
  return results


def download_input(client, params):
  start_time = time.time()
  cexec(client, "s3cmd get s3://{0:s}/{1:s} {1:s}".format(params["input_bucket"], params["key"]))
  end_time = time.time()
  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  return results


def run_ec2_script(client, params):
  start_time = time.time()
  cmd = "python3 ec2_script.py --file {0:s} --application {1:s} --bucket {2:s}".format(params["input_name"], params["ec2"]["application"], params["bucket"])
  print(cmd)
  stdout = cexec(client, cmd)
  end_time = time.time()
  print(stdout)
  duration = end_time - start_time

  regex = re.compile("([A-Z]+) DURATION: ([0-9\.]+)")
  lines = stdout.split("\n")
  stats = []
  for line in lines:
    m = regex.search(line)
    if m:
      duration = float(m.group(2))
      milliseconds = duration * 1000
      stats.append({
        "billed_duration": [milliseconds],
        "cost": duration * MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]],
        "max_duration": milliseconds,
        "memory_used": 0,
        "name": m.group(1).lower()
      })

  return stats


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
  start_time = time.time()
  if not util.is_set(params, "trigger"):
    upload_duration = upload_input(params)[1]
  else:
    upload_duration = 0

  stats = []
  stats.append(load_stats(upload_duration))

  create_stats = create_instance(params)
  instance = create_stats["instance"]
  ec2 = create_stats["ec2"]
  del create_stats["instance"]
  del create_stats["ec2"]
  stats.append(create_stats)

  initiate_stats = initiate_instance(ec2, instance, params)
  client = initiate_stats["client"]
  del initiate_stats["client"]
  stats.append(initiate_stats)

  stats.append(setup_instance(client, params))
  stats += run_ec2_script(client, params)
  end_time = time.time()
  terminate_stats = terminate_instance(instance, client, params)
  stats.append(terminate_stats)

  total_duration = end_time - start_time
  results = [upload_duration, total_duration]
  if params["stats"]:
    results = [stats] + results

  return results

#############################
#           MAIN            #
#############################


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  run(params, 0)


if __name__ == "__main__":
  main()

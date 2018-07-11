import argparse
import boto3
from botocore.client import Config
import constants
import datetime
from enum import Enum
import json
import os
import paramiko
import re
import sort
import subprocess
import time
import util

MASS = re.compile("Z\s+([0-9\.]+)\s+([0-9\.]+)")
MEMORY_PARAMETERS = json.loads(open("json/memory.json").read())
CHECKS = json.loads(open("json/checks.json").read())
REPORT = re.compile(".*Duration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Memory Size:\s([0-9]+)\sMB.*Max Memory Used:\s([0-9]+)\sMB.*")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")
STAT_FIELDS = ["cost", "max_duration", "billed_duration", "memory_used"]

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

  tide_file = "spectra-{0:f}-1-1-1.txt".format(params["now"])
  bucket_name = "maccoss-human-combine-spectra"
  bucket = s3.Bucket(bucket_name)
  for obj in bucket.objects.all():
    if obj.key == tide_file:
      content = obj.get()["Body"].read().decode("utf-8")
      num_lines = len(content.split("\n"))
      print("key", tide_file, "num_lines", num_lines)

  bucket_name = "maccoss-human-output-spectra"
  bucket = s3.Bucket(bucket_name)
  for item in ["peptides", "psms"]:
    key = "percolator.target.{0:s}.txt".format(item)
    output_file = "percolator.target.{0:s}.{1:f}.txt".format(item, params["now"])
    obj = s3.Object(bucket_name, output_file)
    content = obj.get()["Body"].read().decode("utf-8")

    lines = content.split("\n")[1:]
    lines = list(filter(lambda line: len(line.strip()) > 0, lines))
    qvalues = list(map(lambda line: float(line.split("\t")[7]), lines))
    count = len(list(filter(lambda qvalue: qvalue <= CHECKS["qvalue"], qvalues)))
    print("key", key, "qvalues", count)


def get_stages(params):
  stages = MergeLambdaStage
  if not params["lambda"]:
    if params["sort"] == "yes":
      stages = SortEc2Stage
    else:
      stages = NoSortEc2Stage
  elif params["sort"] != "yes":
    stages = NonMergeLambdaStage
  return stages


def print_run_information():
  git_output = subprocess.check_output("git log --oneline | head -n 1", shell=True).decode("utf-8").strip()
  print("Current Git commit", git_output, "\n")


def create_client(params):
  client = setup_client("lambda", params)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
  return client


def process_params(params):
  params["output_bucket"] = params["percolator"]["output_bucket"]
  if params["lambda"]:
    params["input_bucket"] = "maccoss-human-input-spectra"
  else:
    params["input_bucket"] = "maccoss-human-output-spectra"
    if params["sort"] == "pre":
      params["sort"] = "no"

  if params["sort"] == "pre":
    params["input"] = "sort_{0:s}".format(params["input_name"])
  else:
    params["input"] = params["input_name"]

  if params["sort"] != "yes":
    params["split_spectra"]["output_bucket"] = params["merge_spectra"]["output_bucket"]
    if params["lambda"]:
      params["buckets"] = ["input", "split", "analyze", "combine", "output"]
    else:
      params["buckets"] = ["output"]
  else:
    params["buckets"] = ["input", "split", "sort", "merge", "analyze", "combine", "output"]


def process_iteration_params(params, iteration):
  now = time.time()
  params["now"] = now
  params["key"] = util.file_name(params["now"], 1, 1, 1, "ms2")


def upload_input(params):
  bucket_name = params["input_bucket"]
  key = util.file_name(params["now"], 1, 1, 1, "ms2")
  s3 = setup_connection("s3", params)

  start = time.time()
  print("Uploading {0:s} to S3".format(params["input"]))
  s3.Object(bucket_name, key).put(Body=open(params["input"], 'rb'))
  end = time.time()

  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print(key, "last modified", timestamp)
  seconds = end - start
  milliseconds = seconds * 1000

  return int(timestamp), milliseconds


def load_stats(upload_duration):
  return {
    "billed_duration": 0,
    "max_duration": upload_duration,
    "memory_used": 0,
    "cost": 0
  }


def run(params):
  print_run_information()
  process_params(params)
  client = create_client(params)

  iterations = params["iterations"]

  if params["sort"] == "pre":
    args = argparse.Namespace()
    args.file = params["input_name"]
    sort.run(args)

  if params["lambda"]:
    upload_functions(client, params)

  stats = []
  stages = get_stages(params)
  for stage in stages:
    stats.append([])

  for i in range(iterations):
    print("Iteration {0:d}".format(i))
    process_iteration_params(params, i)
    done = False
    while not done:
      try:
        if params["lambda"]:
          results = lambda_benchmark(params)
        else:
          results = ec2_benchmark(params)

        assert(len(results) == len(stages))
        for i in range(len(results)):
          stats[i].append(results[i])

        done = True
      except BenchmarkException as e:
        print("Error during iteration {0:d}".format(i), e)
        clear_buckets(params)

    if params["check_output"]:
      check_output(params)

    clear_buckets(params)
    print("--------------------------\n")

  print("END RESULTS ({0:d} ITERATIONS)".format(iterations))
  for stage in stages:
    print("AVERAGE {0:s} RESULTS".format(stage.name))
    print_stats(calculate_average_results(stats[stage.value], iterations))
    print("")


def calculate_total_stats(stats):
  total_stats = {}

  for field in STAT_FIELDS:
    total_stats[field] = 0

  for stat in stats:
    for field in STAT_FIELDS:
      total_stats[field] += stat[field]

  return total_stats


def calculate_average_results(stats, iterations):
  total_stats = calculate_total_stats(stats)
  average_stats = {}

  for field in STAT_FIELDS:
    average_stats[field] = float(total_stats[field]) / iterations

  return average_stats


def print_stats(stats):
  print("Total Cost", stats["cost"])
  print("Total Runtime", stats["max_duration"] / 1000, "seconds")
  print("Total Billed Duration", stats["billed_duration"] / 1000, "seconds")
  print("Total Memory Used", stats["memory_used"], "MB")


def get_credentials():
  home = os.path.expanduser("~")
  f = open("{0:s}/.aws/credentials".format(home))
  lines = f.readlines()
  access_key = lines[1].split("=")[1].strip()
  secret_key = lines[2].split("=")[1].strip()
  return access_key, secret_key


def setup_connection(service, params):
  [access_key, secret_key] = get_credentials()
  session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=params["region"]
  )
  return session.resource(service)


def clear_buckets(params):
  s3 = setup_connection("s3", params)

  ts = "{0:f}".format(params["now"])
  for bn in params["buckets"]:
    count = 0
    bucket_name = "maccoss-human-{0:s}-spectra".format(bn)
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
      if ts in obj.key:
        obj.delete()
        count += 1
    print("Deleted", count, bn, "objects")


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


def upload_functions(client, params):
  functions = [
    "split_spectra",
    "sort_spectra",
    "merge_spectra",
    "analyze_spectra",
    "combine_spectra_results",
    "percolator"
  ]

  os.chdir("lambda")
  for function in functions:
    fparams = params[function]

    f = open("{0:s}.json".format(function), "w")
    f.write(json.dumps(fparams))
    f.close()

    subprocess.call("zip {0:s}.zip {0:s}.py {0:s}.json ../util.py ../constants.py".format(function), shell=True)

    with open("{0:s}.zip".format(function), "rb") as f:
      zipped_code = f.read()

    response = client.update_function_code(
      FunctionName=fparams["name"],
      ZipFile=zipped_code,
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    response = client.update_function_configuration(
      FunctionName=fparams["name"],
      Timeout=fparams["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

  os.chdir("..")
  print("")


def setup_client(service, params):
  extra_time = 20
  [access_key, secret_key] = get_credentials()
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client(service,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        region_name=params["region"],
                        config=config
                        )
  return client


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
      print("{0:s}: Waiting for {1:s} function{2:s}...".format(now, prefix, suffix))
      if (end - start).total_seconds() > timeout:
        raise BenchmarkException("Could not find bucket {0:s} prefix {1:s}".format(bucket_name, prefix))
      time.sleep(30)
    else:
      print("{0:s}: Found {1:s} function{2:s}".format(now, prefix, suffix))


def wait_for_completion(start_time, params):
  client = setup_client("s3", params)

  overhead = 3.5  # Give ourselves time as we need to wait for the split and analyze functions to finish.
  bucket_name = "maccoss-human-combine-spectra"
  check_objects(client, bucket_name, "spectra", 1, params["combine_spectra_results"]["timeout"] * overhead, params)
  bucket_name = "maccoss-human-output-spectra"
  check_objects(client, bucket_name, "percolator.decoy", 2,  params["percolator"]["timeout"] * overhead, params)

  target_timeout = 30  # Target should be created around the same time as decoys
  check_objects(client, bucket_name, "percolator.target", 2, target_timeout, params)
  print("")
  time.sleep(10)  # Wait a little to make sure percolator logs are on the server


def fetch(client, num_events, log_name, start_time, filter_pattern, extra_args={}):
  events = []
  next_token = None
  while len(events) < num_events:
    args = {
      "filterPattern": filter_pattern,
      "limit": num_events - len(events),
      "logGroupName": "/aws/lambda/{0:s}".format(log_name),
      "startTime": start_time
    }
    args = {**args, **extra_args}

    if next_token:
      args["nextToken"] = next_token

    response = client.filter_log_events(**args)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
      print(response)
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

    if "nextToken" not in response:
      break
    next_token = response["nextToken"]
    events += response["events"]

  return events


def fetch_events(client, num_events, log_name, start_time, filter_pattern, extra_args={}):
  events = fetch(client, num_events, log_name, start_time, filter_pattern, extra_args)
  if len(events) != num_events:
    print(log_name, len(events), num_events)
    #  error_events = fetch(client, num_events - len(events), log_name, start_time, "*Task timed out after*", extra_args)
    raise BenchmarkException("sad")  # log_name, "has", len(error_events), "timeouts", extra_args)
  return events


def calculate_cost(duration, memory_size):
  # Cost per 100ms
  millisecond_cost = MEMORY_PARAMETERS["lambda"][str(memory_size)]
  return int(duration / 100) * millisecond_cost


def parse_split_logs(client, start_time, params):
  sparams = params["split_spectra"]
  events = fetch_events(client, 1, sparams["name"], start_time, "REPORT RequestId")
  m = REPORT.match(events[0]["message"])
  duration = int(m.group(2))
  memory_used = int(m.group(4))
  cost = calculate_cost(duration, sparams["memory_size"])

  print("Split Spectra")
  print("Timestamp", events[0]["timestamp"])
  print("Billed Duration", duration, "milliseconds")
  print("Max Memory Used", m.group(4))
  print("Cost", cost)
  print("")

  return {
    "billed_duration": duration,
    "max_duration": duration,
    "memory_used": memory_used,
    "cost": cost
  }


def parse_mult_logs(client, start_time, params, lambda_name):
  num_spectra = int(subprocess.check_output("cat sorted_{0:s} | grep 'S\s' | wc -l".format(params["input_name"]), shell=True).decode("utf-8").strip())
  batch_size = params["split_spectra"]["batch_size"]
  lparams = params[lambda_name]
  num_lambdas = int((num_spectra + batch_size - 1) / batch_size)
  events = fetch_events(client, num_lambdas, lparams["name"], start_time, "REPORT RequestId")
  max_billed_duration = 0
  total_billed_duration = 0
  total_memory_used = 0  # TODO: Handle
  min_timestamp = events[0]["timestamp"]
  max_timestamp = events[0]["timestamp"]

  for event in events:
    min_timestamp = min(min_timestamp, event["timestamp"])
    max_timestamp = max(max_timestamp, event["timestamp"])
    m = REPORT.match(event["message"])
    duration = int(m.group(2))
    memory_used = int(m.group(4))
    max_billed_duration = max(max_billed_duration, duration)
    total_billed_duration += duration
    total_memory_used += memory_used

  cost = calculate_cost(total_billed_duration, lparams["memory_size"])

  print(lambda_name)
  print("Min Timestamp", min_timestamp)
  print("Max Timestamp", max_timestamp)
  print("Max Billed Duration", max_billed_duration, "milliseconds")
  print("Total Billed Duration", total_billed_duration, "milliseconds")
  print("Cost", cost)
  print("")

  return {
    "billed_duration": total_billed_duration,
    "max_duration": max_billed_duration,
    "memory_used": total_memory_used,
    "cost": cost
  }


def parse_sort_logs(client, start_time, params):
  return parse_mult_logs(client, start_time, params, "sort_spectra")


def parse_merge_logs(client, start_time, params):
  return parse_mult_logs(client, start_time, params, "merge_spectra")


def parse_analyze_logs(client, start_time, params):
  return parse_mult_logs(client, start_time, params, "analyze_spectra")


def parse_combine_logs(client, start_time, params):
  cparams = params["combine_spectra_results"]
  name = cparams["name"]
  combine_events = fetch_events(client, 1, name, start_time, "Combining")

  extra_args = {
    "logStreamNames": [combine_events[0]["logStreamName"]],
  }
  events = fetch_events(client, 1, name, combine_events[0]["timestamp"], "REPORT RequestId", extra_args)

  m = REPORT.match(events[0]["message"])
  duration = int(m.group(2))
  memory_used = int(m.group(4))
  cost = calculate_cost(duration, cparams["memory_size"])

  print("Combine Spectra")
  print("Timestamp", combine_events[0]["timestamp"])
  print("Billed Duration", duration, "milliseconds")
  print("Max Memory Used", m.group(4))
  print("Cost", cost)
  print("")

  return {
    "billed_duration": duration,
    "max_duration": duration,
    "memory_used": memory_used,
    "cost": cost
  }


def parse_percolator_logs(client, start_time, params):
  pparams = params["percolator"]
  events = fetch_events(client, 1, pparams["name"], start_time, "REPORT RequestId")
  m = REPORT.match(events[0]["message"])
  duration = int(m.group(2))
  memory_used = int(m.group(4))
  cost = calculate_cost(duration, pparams["memory_size"])

  print("Percolator Spectra")
  print("Timestamp", events[0]["timestamp"])
  print("Billed Duration", duration, "milliseconds")
  print("Max Memory Used", m.group(4))
  print("Cost", cost)
  print("")

  return {
    "billed_duration": duration,
    "max_duration": duration,
    "memory_used": memory_used,
    "cost": cost
  }


def parse_logs(params, upload_timestamp, upload_duration):
  client = setup_client("logs", params)

  stats = []
  stats.append(load_stats(upload_duration))
  stats.append(parse_split_logs(client, upload_timestamp, params))

  if params["sort"] == "yes":
    stats.append(parse_sort_logs(client, upload_timestamp, params))
    stats.append(parse_merge_logs(client, upload_timestamp, params))

  stats.append(parse_analyze_logs(client, upload_timestamp, params))
  stats.append(parse_combine_logs(client, upload_timestamp, params))
  stats.append(parse_percolator_logs(client, upload_timestamp, params))

  total_stats = calculate_total_stats(stats)
  print("END RESULTS")
  print_stats(total_stats)
  stats.append(total_stats)

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
  ec2 = setup_connection("ec2", params)
  ami = params["ec2"]["default_ami"]
  if params["ec2"]["use_ami"]:
    ami = params["ec2"]["ami"]

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


def cexec(client, command):
  (stdin, stdout, stderr) = client.exec_command(command)
  stdout.channel.recv_exit_status()
  return stdout.read()


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
    [access_key, secret_key] = get_credentials()
    cexec(client, "echo -e '{0:s}\n{1:s}\n\n\n\n\nY\ny\n' | s3cmd --configure".format(access_key, secret_key))
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


def download_results(client, params):
  start_time = time.time()
  cexec(client, "s3cmd get s3://{0:s}/{1:s} {1:s}".format(params["input_bucket"], params["key"]))
  end_time = time.time()
  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  return results


def sort_spectra(client, params):
  start_time = time.time()
  cexec(client, "python sort.py --file {0:s}".format(params["key"]))
  params["key"] = "sorted_{0:s}".format(params["key"])
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def run_analyze(client, params):
  print("Running Tide")
  arguments = [
    "--num-threads", str(params["analyze_spectra"]["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", "tide-output",
  ]
  start_time = time.time()
  command = "sudo ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(params["key"], " ".join(arguments))
  cexec(client, command)
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def run_percolator(client, params):
  print("Running Percolator")
  arguments = [
    "--subset-max-train", str(params["percolator"]["max_train"]),
    "--quick-validation", "T",
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
    output_file = "percolator.target.{0:s}.{1:f}.txt".format(item, params["now"])
    cexec(client, "s3cmd put crux-output/{0:s} s3://{1:s}/{2:s}".format(input_file, bucket_name, output_file))

  input_file = "tide-output/tide-search.txt"
  output_file = "spectra-{0:f}-1-1-1.txt".format(params["now"])
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
  stats.append(download_results(client, params))
  if params["sort"] == "yes":
    print("sorting")
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
  run(params)


if __name__ == "__main__":
  main()

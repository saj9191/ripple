import argparse
import boto3
from botocore.client import Config
import datetime
from enum import Enum
import json
import os
import paramiko
import re
import subprocess
import time

MASS = re.compile("Z\s+([0-9\.]+)\s+([0-9\.]+)")
MEMORY_PARAMETERS = json.loads(open("json/memory.json").read())
REPORT = re.compile(".*Duration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Memory Size:\s([0-9]+)\sMB.*Max Memory Used:\s([0-9]+)\sMB.*")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")
STAT_FIELDS = ["cost", "max_duration", "billed_duration", "memory_used"]

#############################
#         COMMON            #
#############################


class BenchmarkException(Exception):
  pass


def run(params):
  git_output = subprocess.check_output("git log --oneline | head -n 1", shell=True).decode("utf-8").strip()
  print("Current Git commit", git_output)
  iterations = params["iterations"]
  client = setup_client("lambda", params)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

  if params["sort"]:
    sort_spectra(params["input_name"])

  upload_functions(client, params)

  stats = []

  stages = LambdaStage
  if not params["lambda"]:
    stages = Ec2Stage

  for stage in stages:
    stats.append([])

  for i in range(iterations):
    print("Iteration {0:d}".format(i))
    done = False
    while not done:
      try:
        if params["lambda"]:
          results = lambda_benchmark(params)
        else:
          results = ec2_benchmark(params)

        for i in range(len(results)):
          stats[i].append(results[i])
        done = True
      except BenchmarkException:
        print("Error during iteration {0:d}".format(i))
        done = False

    print("--------------------------")
    print("")

  print("END RESULTS ({0:d} ITERATIONS)".format(iterations))
  for stage in stages:
    print("AVERAGE {0:s} RESULTS".format(stage.name))
    print_stats(calculate_average_results(stats[stage.value], iterations))


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
  print("Total Runtime", stats["max_duration"], "milliseconds")
  print("Total Billed Duration", stats["billed_duration"], "milliseconds")
  print("Total Memory Used", stats["memory_used"], "MB")


def get_credentials():
  f = open("/home/shannon/.aws/credentials")
  lines = f.readlines()
  access_key = lines[1].split("=")[1].strip()
  secret_key = lines[2].split("=")[1].strip()
  return access_key, secret_key


def setup_connection(service, params):
  [access_key, secret_key] = get_credentials()
  session = boto3.Session(
      aws_access_key_id=access_key,
      aws_secret_access_key=secret_key
  )
  return session.resource(service)


def clear_buckets(params):
  s3 = setup_connection("s3", params)
  for bucket_name in ["maccoss-human-input-spectra", "maccoss-human-split-spectra", "maccoss-human-output-spectra"]:
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()


#############################
#         LAMBDA            #
#############################

class LambdaStage(Enum):
  LOAD = 0
  SPLIT = 1
  ANALYZE = 2
  COMBINE = 3
  PERCOLATOR = 4
  TOTAL = 5


def upload_functions(client, params):
  functions = ["split_spectra", "analyze_spectra", "combine_spectra_results", "percolator"]

  os.chdir("lambda")
  for function in functions:
    fparams = params[function]

    f = open("{0:s}.json".format(function), "w")
    f.write(json.dumps(fparams))
    f.close()

    subprocess.call("zip {0:s}.zip {0:s}.py {0:s}.json util.py".format(function), shell=True)

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


def sort_spectra(name):
  f = open(name)

  spectrum = []
  mass = None
  spectra = []

  line = f.readline()
  while line:
    m = SPECTRA.match(line)
    if m:
      if mass is not None:
        spectra.append((mass, "".join(spectrum)))
        mass = None
        spectrum = []

    m = MASS.match(line)
    if m:
      mass = float(m.group(2))
    spectrum.append(line)
    line = f.readline()

  f.close()

  def getMass(spectrum):
    return spectrum[0]

  spectra.sort(key=getMass)

  sorted_name = "sorted_{0:s}".format(name)
  f = open(sorted_name, "w+")
  f.write("H Extractor MzXML2Search\n")
  for spectrum in spectra:
    for line in spectrum[1]:
      f.write(line)


def upload_input(params):
  bucket_name = "maccoss-human-input-spectra"
  key = "sorted_{0:s}".format(params["input_name"])
  s3 = setup_connection("s3", params)
  start = time.time()
  s3.Object(bucket_name, key).put(Body=open(key, 'rb'))
  end = time.time()
  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print(key, "last modified", timestamp)
  return int(timestamp), end - start


def check_objects(client, bucket_name, prefix, count, timeout):
  done = False
  suffix = ""
  if count > 1:
    suffix = "s"

  start = datetime.datetime.now()
  while not done:
    response = client.list_objects(
      Bucket=bucket_name,
      Prefix=prefix
    )
    done = (("Contents" in response) and (len(response["Contents"]) == count))
    end = datetime.datetime.now()
    now = end.strftime("%H:%M:%S")
    if not done:
      print("{0:s}: Waiting for {1:s} function{2:s}...".format(now, prefix, suffix))
      if (end - start).total_seconds() > timeout:
        raise BenchmarkException("Could not find bucket {0:s} prefix {1:s}".format(bucket_name, prefix))
      time.sleep(60)
    else:
      print("{0:s}: Found {1:s} function{2:s}".format(now, prefix, suffix))


def wait_for_completion(start_time, params):
  client = setup_client("s3", params)
  bucket_name = "maccoss-human-output-spectra"

  overhead = 3.5  # Give ourselves time as we need to wait for the split and analyze functions to finish.
  check_objects(client, bucket_name, "combined", 1, params["combine_spectra_results"]["timeout"] * overhead)
  check_objects(client, bucket_name, "decoy", 2, params["percolator"]["timeout"] * overhead)

  target_timeout = 30  # Target should be created around the same time as decoys
  check_objects(client, bucket_name, "target", 2, target_timeout)
  print("")


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
    error_events = fetch(client, num_events - len(events), log_name, start_time, "*Task timed out after*", extra_args)
    raise BenchmarkException(log_name, "has", len(error_events), "timeouts", extra_args)
  return events


def calculate_cost(duration, memory_size):
  # Cost per 100ms
  millisecond_cost = MEMORY_PARAMETERS[str(memory_size)]
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


def parse_analyze_logs(client, start_time, params):
  num_spectra = int(subprocess.check_output("cat sorted_{0:s} | grep 'S\s' | wc -l".format(params["input_name"]), shell=True).decode("utf-8").strip())
  aparams = params["analyze_spectra"]
  batch_size = params["split_spectra"]["batch_size"]
  num_lambdas = int((num_spectra + batch_size - 1) / batch_size)
  events = fetch_events(client, num_lambdas, aparams["name"], start_time, "REPORT RequestId")
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

  cost = calculate_cost(total_billed_duration, aparams["memory_size"])

  print("Analyze Spectra")
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

  load_stats = {
    "billed_duration": 0,
    "max_duration": upload_duration,
    "memory_used": 0,
    "cost": 0
  }
  stats.append(load_stats)

  split_stats = parse_split_logs(client, upload_timestamp, params)
  stats.append(split_stats)

  analyze_stats = parse_analyze_logs(client, upload_timestamp, params)
  stats.append(analyze_stats)

  combine_stats = parse_combine_logs(client, upload_timestamp, params)
  stats.append(combine_stats)

  percolator_stats = parse_percolator_logs(client, upload_timestamp, params)
  stats.append(percolator_stats)

  total_stats = calculate_total_stats(stats)
  print("END RESULTS")
  print_stats(total_stats)

  return (load_stats, split_stats, analyze_stats, combine_stats, percolator_stats, total_stats)


def lambda_benchmark(params):
  clear_buckets(params)
  [upload_timestamp, upload_duration] = upload_input(params)
  wait_for_completion(upload_timestamp, params)
  return parse_logs(params, upload_timestamp, upload_duration)

#############################
#           EC2             #
#############################


class Ec2Stage(Enum):
  CREATE = 0
  LOAD = 1
  TIDE = 2
  PERCOLATOR = 3
  UPLOAD = 4
  TERMINATE = 5
  TOTAL = 6


def create_instance(params):
  ec2 = boto3.resource("ec2")
  start_time = time.time()
  instances = ec2.create_instances(
    ImageId="ami-0ad99772",
    InstanceType=params["ec2"]["type"],
    KeyName=params["ec2"]["key"],
    MinCount=1,
    MaxCount=1
  )
  assert(len(instances) == 1)
  instance = instances[0]
  instance.wait_util_running()
  end_time = time.time()
  duration = end_time - start_time

  return {
    "duration": duration,
    "billed_duration": duration,
    "memory_used": 0,
    "cost": 0,
    "instance": instance
  }


def setup_instance(instance, params):
  ec2_dir = "/home/ec2"
  client = paramiko.SSHClient()
  client.connect(client.public_ip_address, username="ubuntu", key_filename=params["ec2"]["key"], timeout=params["ec2"]["timeout"])
  client.execute_command("sudo apt-get install s3cmd --yes")
  sftp = client.open_sftp()

  start_time = time.time()
  sftp.put("crux", ec2_dir)
  sftp.put("HUMAN.fasta.20170123", ec2_dir)

  index_dir = "{0:s}/HUMAN.fasta.20170123.index".format(ec2_dir)
  sftp.mkdir(index_dir)
  for item in os.listdir("HUMAN.fasta.20170123.index"):
    sftp.put(item, "{0:s}/{1:s}".format(index_dir, item))
  sftp.put(params["input_name"], ec2_dir)
  end_time = time.time()

  sftp.close()
  duration = end_time - start_time

  return {
    "duration": duration,
    "billed_duration": duration,
    "memory_used": 0,
    "cost": 0,
    "client": client
  }


def run_analyze(client, params):
  arguments = [
    "--num-threads", str(params["analyze_spectra"]["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
  ]
  start_time = time.time()
  command = "./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(params["input_name"], " ".join(arguments))
  (stdin, stdout, stderr) = client.execute_command(command)
  print("stdin", stdin)
  print("stdout", stdout)
  print("stderr", stderr)
  end_time = time.time()
  duration = end_time - start_time

  return {
    "duration": duration,
    "billed_duration": duration,
    "memory_used": 0,
    "cost": 0
  }


def run_percolator(client, params):
  arguments = [
    "--subset-max-train", str(params["percolator"]["max_train"]),
    "--quick-validation", "T",
  ]

  start_time = time.time()
  (stdin, stdout, stderr) = client.execute_command("./crux percolator {0:s} {1:s}".format("crux-output/tide-search.txt"), " ".join(arguments))
  print("stdin", stdin)
  print("stdout", stdout)
  print("stderr", stderr)
  end_time = time.time()
  duration = end_time - start_time

  return {
    "duration": duration,
    "billed_duration": duration,
    "memory_used": 0,
    "cost": 0
  }


def upload_results(client, params):
  start_time = time.time()
  for pep in ["decoy", "target"]:
    for item in ["peptides", "psms"]:
      file = "percolator.{0:s}.{1:s}.txt".format(pep, item)
      client.execute_command("s3cmd put crux-output/{0:s} s3://maccoss-human-output-spectra/{0:s}".format(file))
  end_time = time.time()
  duration = end_time - start_time

  return {
    "duration": duration,
    "billed_duration": duration,
    "memory_used": 0,
    "cost": 0
  }


def ec2_benchmark(params):
  clear_buckets(params)
  create_stats = create_instance(params)
  instance = create_stats["instance"]
  setup_stats = setup_instance(instance, params)
  client = setup_stats["client"]
  analyze_stats = run_analyze(client, params)
  percolator_stats = run_percolator(client, params)
  upload_stats = upload_results(client, params)

  return (create_stats, setup_stats, analyze_stats, percolator_stats, upload_stats)

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

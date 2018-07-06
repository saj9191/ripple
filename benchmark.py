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
CHECKS = json.loads(open("json/checks.json").read())
REPORT = re.compile(".*Duration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Memory Size:\s([0-9]+)\sMB.*Max Memory Used:\s([0-9]+)\sMB.*")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")
STAT_FIELDS = ["cost", "max_duration", "billed_duration", "memory_used"]

#############################
#         COMMON            #
#############################


class BenchmarkException(Exception):
  pass


def check_output(params):
  bucket_name = "maccoss-human-output-spectra"
  s3 = setup_connection("s3", params)
  for key in CHECKS:
    obj = s3.Object(bucket_name, key)
    content = obj.get()["Body"].read().decode("utf-8")
    num_lines = len(content.split("\n"))
    assert(num_lines == key[params["input_name"]]["num_lines"])


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

  if params["lambda"]:
    upload_functions(client, params)

  stats = []

  stages = LambdaStage
  if not params["lambda"]:
    stages = Ec2Stage

  for stage in stages:
    stats.append([])

  for i in range(iterations):
    print("Iteration {0:d}".format(i))
    now = time.time()
    key = "{0:f}_{1:s}".format(now, params["input_name"])
    params["input_key"] = key
    params["now"] = now
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

    check_output(params)

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
  print("Total Runtime", stats["max_duration"] / 1000, "seconds")
  print("Total Billed Duration", stats["billed_duration"] / 1000, "seconds")
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
    aws_secret_access_key=secret_key,
    region_name=params["region"]
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
    if not line.startswith("H"):
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
  s3.Object(bucket_name, params["input_key"]).put(Body=open(key, 'rb'))
  end = time.time()
  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print(key, "last modified", timestamp)
  seconds = end - start
  milliseconds = seconds * 1000
  return int(timestamp), milliseconds


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
        "Value": "maccoss-benchmark-{0:f}".format(time.time())
      }]
    }]
  )
  assert(len(instances) == 1)
  instance = instances[0]
  print("Waiting for instance to initiate")
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


def setup_instance(ec2, instance, params):
  start_time = time.time()

  print("Wait for status checks")
  instance_status = list(ec2.meta.client.describe_instance_status(InstanceIds=[instance.id])["InstanceStatuses"])[0]
  while instance_status["InstanceStatus"]["Details"][0]["Status"] == "initializing":
    instance_status = list(ec2.meta.client.describe_instance_status(InstanceIds=[instance.id])["InstanceStatuses"])[0]
    time.sleep(1)
  assert(instance_status["InstanceStatus"]["Details"][0]["Status"] == "passed")

  instance.reload()

  client = paramiko.SSHClient()
  pem = params["ec2"]["key"] + ".pem"
  key = paramiko.RSAKey.from_private_key_file(pem)
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  user = "ec2-user"

  client.connect(
    instance.public_ip_address,
    username=user,
    pkey=key
  )

  sftp = client.open_sftp()
  items = []
  if not params["ec2"]["use_ami"]:
    cexec(client, "cd /etc/yum.repos.d; sudo wget http://s3tools.org/repo/RHEL_6/s3tools.repo")
    cexec(client, "sudo yum -y install s3cmd")
    cexec(client, "sudo update-alternatives --set python /usr/bin/python2.6")
    [access_key, secret_key] = get_credentials()
    cexec(client, "echo -e '{0:s}\n{1:s}\n\n\n\n\nY\ny\n' | s3cmd --configure".format(access_key, secret_key))
    items.append("crux")
    items.append("HUMAN.fasta.20170123")

    index_dir = "HUMAN.fasta.20170123.index"
    sftp.mkdir(index_dir)
    for item in os.listdir(index_dir):
      path = "{0:s}/{1:s}".format(index_dir, item)
      sftp.put(path, path)

  sftp.put("sorted_{0:s}".format(params["input_name"]), params["input_key"])
  for item in items:
    sftp.put(item, item)

  if not params["ec2"]["use_ami"]:
    cexec(client, "sudo chmod +x crux")

  sftp.close()
#  command = "cd aws-scripts-mon; cp awscreds.template awscreds.conf; echo 'AWSAccessKeyId={0:s}\nAWSSecretKey={1:s}' >> awscreds.conf"
#  cexec(client, command)
  end_time = time.time()

  duration = end_time - start_time
  results = calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])
  results["client"] = client
  return results


def run_analyze(client, params):
  print("Running Tide")
  arguments = [
    "--num-threads", str(params["analyze_spectra"]["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
  ]
  start_time = time.time()
  command = "sudo ./crux tide-search sorted_{0:s} HUMAN.fasta.20170123.index {1:s}".format(params["input_key"], " ".join(arguments))
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
  cexec(client, "sudo ./crux percolator {0:s} {1:s}".format("crux-output/tide-search.txt", " ".join(arguments)))
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def upload_results(client, params):
  print("Uploading files to s3")
  bucket_name = "maccoss-human-output-spectra"
  start_time = time.time()
  for pep in ["decoy", "target"]:
    for item in ["peptides", "psms"]:
      input_file = "percolator.{0:s}.{1:s}.txt".format(pep, item)
      output_file = "percolator.{0:s}.{1:s}.{2:f}.txt".format(pep, item, params["now"])
      cexec(client, "s3cmd put crux-output/{0:s} s3://{1:s}/{2:s}".format(input_file, bucket_name, output_file))
  end_time = time.time()
  duration = end_time - start_time

  s3 = setup_connection("s3", params)
  bucket = s3.Bucket(bucket_name)
  assert(sum(1 for _ in bucket.objects.all()) == 4)

  return calculate_results(duration, MEMORY_PARAMETERS["ec2"][params["ec2"]["type"]])


def terminate_instance(instance, client, params):
  start_time = time.time()
#  cexec(client, "cd aws-scripts-mon; ./mon-put-instance-data.pl --mem-used-incl-cache-buff --mem-util --mem-used --mem-avail")
  client.close()
  instance.terminate()
  instance.wait_until_terminated()
  end_time = time.time()
  duration = end_time - start_time

  return calculate_results(duration, 0)


def ec2_benchmark(params):
  clear_buckets(params)
  stats = []

  if params["sort"]:
    sort_spectra(params["input_name"])

  create_stats = create_instance(params)
  stats.append(create_stats)

  instance = create_stats["instance"]
  ec2 = create_stats["ec2"]
  setup_stats = setup_instance(ec2, instance, params)
  stats.append(setup_stats)

  client = setup_stats["client"]
  analyze_stats = run_analyze(client, params)
  stats.append(analyze_stats)

  percolator_stats = run_percolator(client, params)
  stats.append(percolator_stats)

  upload_stats = upload_results(client, params)
  stats.append(upload_stats)

  terminate_stats = terminate_instance(instance, client, params)
  stats.append(terminate_stats)

  total_stats = calculate_total_stats(stats)
  print("END RESULTS")
  print_stats(total_stats)

  return (create_stats, setup_stats, analyze_stats, percolator_stats, upload_stats, terminate_stats, total_stats)

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

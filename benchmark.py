import argparse
import boto3
from botocore.client import Config
import datetime
import json
import numpy
import os
import queue
import re
import subprocess
import sys
import threading
import time

REPORT = re.compile(".*Duration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Memory Size:\s([0-9]+)\sMB.*Max Memory Used:\s([0-9]+)\sMB.*")
SPECTRA = re.compile("S\s\d+.*")
INTENSITY = re.compile("I\s+MS1Intensity\s+([0-9\.]+)")
MEMORY_PARAMETERS = json.loads(open("json/memory.json").read())

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

# TODO: Remove once we incorporate this into the split lambda function
def process():
  print("process")
  subprocess.call("rm lambda/sorted-small-*", shell=True)
  f = open("lambda/small.ms2")
  lines = f.readlines()[1:]
  f.close()

  spectrum = []
  intensity = None
  spectra = []

  for line in lines:
    if SPECTRA.match(line):
      if intensity is not None:
        spectrum.append((intensity, "".join(spectra)))
        intensity = None
        spectra = []

    m = INTENSITY.match(line)
    if m:
      intensity = float(m.group(1))

    spectra.append(line)

  spectrum = sorted(spectrum, key=lambda spectra: -spectra[0])

  offset = 260
  i = 0
  print("offset", offset)
  while i * offset < min(len(spectrum), 1):
    index = i * offset
    f = open("lambda/sorted-small-{0:d}.ms2".format(i), "w+")
    f.write("H Extractor MzXML2Search\n")
    for spectra in spectrum[index:min(index+offset, len(spectrum))]:
      for line in spectra[1]:
        f.write(line)
    i += 1
  return i

def upload_input():
  bucket_name = "maccoss-human-input-spectra"
  key = "20170403_HelaQC_01.ms2"
  s3 = boto3.resource("s3")
  s3.Object(bucket_name, key).put(Body=open(key, 'rb'))
  obj = s3.Object(bucket_name, key)
  timestamp = obj.last_modified.timestamp() * 1000
  print(key, "last modified", timestamp)
  return int(timestamp)

def check_objects(client, bucket_name, prefix, count):
  done = False
  suffix = ""
  if count > 1:
    suffix = "s"
  while not done:
    response = client.list_objects(
      Bucket=bucket_name,
      Prefix=prefix
    )
    done = (("Contents" in response) and (len(response["Contents"]) == count))
    now = datetime.datetime.now().strftime("%H:%M:%S")
    if not done:
      print("{0:s}: Waiting for {1:s} function{2:s}...".format(now, prefix, suffix))
      time.sleep(60)
    else:
      print("{0:s}: Found {1:s} function{2:s}".format(now, prefix, suffix))

def wait_for_completion(params):
  client = boto3.client("s3", region_name=params["region"])
  bucket_name = "maccoss-human-output-spectra"

  check_objects(client, bucket_name, "combined", 1)
  check_objects(client, bucket_name, "decoy", 2)
  check_objects(client, bucket_name, "target", 2)
  print("")

def fetch_events(client, num_events, log_name, start_time, filter_pattern, extra_args = {}):
  events = []
  next_token = None
  while len(events) < num_events:
    args = {
      "filterPattern": filter_pattern,
      "limit": num_events - len(events),
      "logGroupName": "/aws/lambda/{0:s}".format(log_name),
      "startTime": start_time
    }
    args = { **args, **extra_args }

    if next_token:
      args["nextToken"] = next_token

    response = client.filter_log_events(**args)
    if "nextToken" not in response:
      print(response)
    next_token = response["nextToken"]
    events += response["events"]

  if len(events) != num_events:
    print(response)
  assert(len(events) == num_events)
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
  num_spectra = int(subprocess.check_output("cat 20170403_HelaQC_01.ms2 | grep 'MS1Intensity' | wc -l", shell=True).decode("utf-8").strip())
  aparams = params["analyze_spectra"]
  batch_size = params["split_spectra"]["batch_size"]
  num_lambdas = int((num_spectra + batch_size - 1) / batch_size)
  events = fetch_events(client, num_lambdas, aparams["name"], start_time, "REPORT RequestId")
  max_billed_duration = 0
  total_billed_duration = 0
  total_memory_used = 0 # TODO: Handle
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

STAT_FIELDS = ["cost", "max_duration", "billed_duration", "memory_used"]

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

def parse_logs(params, upload_timestamp):
  client = boto3.client("logs", region_name=params["region"])
  stats = []

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

  return (split_stats, analyze_stats, combine_stats, percolator_stats, total_stats)

def clear_buckets():
  s3 = boto3.resource("s3")
  for bucket_name in ["maccoss-human-input-spectra", "maccoss-human-split-spectra", "maccoss-human-output-spectra"]:
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()

def benchmark(params):
  clear_buckets()
  upload_timestamp = upload_input()
  wait_for_completion(params)
  return parse_logs(params, upload_timestamp)

def run(params):
  print("Current Git commit", subprocess.check_output("git rev-parse HEAD", shell=True).decode("utf-8").strip())
  iterations = params["iterations"]
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client("lambda", region_name=params["region"], config=config)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

  upload_functions(client, params)

  stats = ([], [], [], [], [])

  for i in range(iterations):
    print("Iteration {0:d}".format(i))
    results = benchmark(params)
    for i in range(len(results)):
      stats[i].append(results[i])

    print("--------------------------")
    print("")

  split_stats = calculate_average_results(stats[0], iterations)
  analyze_stats = calculate_average_results(stats[1], iterations)
  combine_stats = calculate_average_results(stats[2], iterations)
  percolator_stats = calculate_average_results(stats[3], iterations)
  total_stats = calculate_average_results(stats[4], iterations)

  print("END RESULTS ({0:d} ITERATIONS)".format(iterations))
  print("AVERAGE SPLIT RESULTS")
  print_stats(split_stats)
  print("AVERAGE ANALYZE RESULTS")
  print_stats(analyze_stats)
  print("AVERAGE COMBINE RESULTS")
  print_stats(combine_stats)
  print("AVERAGE PERCOLATOR RESULTS")
  print_stats(percolator_stats)
  print("AVERAGE TOTAL RESULTS")
  print_stats(total_stats)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params)

if __name__ == "__main__":
  main()

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

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CWD, "lib"))

REPORT = re.compile(".*Duration:\s([0-9\.]+)\sms.*Billed Duration:\s([0-9\.]+)\sms.*Memory Size:\s([0-9A-Z ]+).*Max Memory Used:\s([0-9A-Z ]+).*")
SPECTRA = re.compile("S\s\d+.*")
INTENSITY = re.compile("I\s+MS1Intensity\s+([0-9\.]+)")

def upload_functions(client, params):
  functions = ["split_spectra", "analyze_spectra", "combine_spectra_results", "percolator"]
  names = ["SplitSpectra", "AnalyzeSpectra", "CombineSpectraResults", "Percolator"]

  os.chdir("lambda")
  for i in range(len(functions)):
    function = functions[i]
    name = names[i]
    subprocess.call("zip {0:s}.zip {0:s}.py".format(function), shell=True)

    with open("{0:s}.zip".format(function), "rb") as f:
      zipped_code = f.read()

    response = client.update_function_code(
      FunctionName=name,
      ZipFile=zipped_code,
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
  print(key, "last modified", obj.last_modified)
  timestamp = obj.last_modified.timestamp()
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

def fetch_events(client, num_events, log_name, start_time, filter_pattern):
  events = []
  next_token = None
  while len(events) < num_events:
    args = {
      "filterPattern": filter_pattern,
      "limit": num_events - len(events),
      "logGroupName": "/aws/lambda/{0:s}".format(log_name),
      "startTime": start_time
    }
    
    if next_token:
      args["nextToken"] = next_token

    response = client.filter_log_events(**args)
    next_token = response["nextToken"]
    events += response["events"]

  assert(len(events) == num_events)
  return events

def parse_split_logs(client, start_time):
  events = fetch_events(client, 1, "SplitSpectra", start_time, "REPORT RequestId") 
  m = REPORT.match(events[0]["message"])
  print("Split Spectra")
  print("Billed Duration", m.group(2), "milliseconds")
  print("Max Memory Used", m.group(4))
  print("")
  return { "billed_duration": int(m.group(2)), "memory_used": m.group(4) }

def parse_analyze_logs(client, start_time):
  num_lambdas = 42 # TODO: Unhardcode
  events = fetch_events(client, num_lambdas, "AnalyzeSpectra", start_time, "REPORT RequestId") 

  max_billed_duration = 0
  max_memory_used = 0 # TODO: Handle
  
  for event in events:
    m = REPORT.match(event["message"])
    if m:
      max_billed_duration = max(max_billed_duration, int(m.group(2)))

  print("Analyze Spectra")
  print("Max Billed Duration", max_billed_duration, "milliseconds")
  print("")
  return max_billed_duration

def parse_combine_logs(client, start_time):
  events = fetch_events(client, 1, "CombineSpectraResults", start_time, "Combining") 
  response = client.filter_log_events(
    logGroupName="/aws/lambda/CombineSpectraResults",
    logStreamNames=[events[0]["logStreamName"]],
    startTime=events[0]["timestamp"],
    filterPattern="REPORT RequestId",
    limit = 1
  )
  assert(len(response["events"]) == 1)
  m = REPORT.match(response["events"][0]["message"])
  print("Combine Spectra")
  print("Billed Duration", m.group(2), "milliseconds")
  print("Max Memory Used", m.group(4))
  print("")
  return { "billed_duration": int(m.group(2)), "memory_used": m.group(4) }

def parse_percolator_logs(client, start_time):
  events = fetch_events(client, 1, "Percolator", start_time, "REPORT RequestId") 
  m = REPORT.match(events[0]["message"])
  print("Percolator Spectra")
  print("Billed Duration", m.group(2), "milliseconds")
  print("Max Memory Used", m.group(4))
  print("")
  return { "billed_duration": int(m.group(2)), "memory_used": m.group(4) }
  
def parse_logs(params, upload_timestamp):
  client = boto3.client("logs", region_name=params["region"])
  split_stats = parse_split_logs(client, upload_timestamp)
  analyze_stats = parse_analyze_logs(client, upload_timestamp)
  combine_stats = parse_combine_logs(client, upload_timestamp)
  percolator_stats = parse_percolator_logs(client, upload_timestamp)

def clear_buckets():
  s3 = boto3.resource("s3")
  for bucket_name in ["maccoss-human-input-spectra", "maccoss-human-split-spectra", "maccoss-human-output-spectra"]:
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()

def benchmark(params):
  clear_buckets()
  upload_timestamp = upload_input()
  wait_for_completion(params)
  parse_logs(params, upload_timestamp)

def run(params):
  num_threads = params["num_threads"]
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client("lambda", region_name=params["region"], config=config)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

  upload_functions(client, params)
  benchmark(params)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters") 
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params)
  
main()

import json
import math
import numpy
import os
import plot
import util

SSW_BYTES_WRITTEN = {}

def get_ec2_runtime_stats(application, key, stats, params):
  runtimes = list(map(lambda s: 0, stats))

  for sindex in range(len(stats)):
    pipeline = stats[sindex]
    runtimes[sindex] = pipeline[-2]["max_duration"] / 1000.0

  return runtimes


def get_ec2_cost_stats(application, key, stats, params):
  global SSW_BYTES_WRITTEN
  memory = json.loads(open("json/memory.json").read())
  s3_costs = list(map(lambda s: 0, stats))
  costs = list(map(lambda s: 0, stats))

  s3 = util.s3(params)
  if params["tag"] == "smith-waterman":
    bucket = "ssw-input"
  else:
    bucket = "shjoyner-als" if key.startswith("TN_") else "shjoyner-ash"

  obj = s3.Object(bucket, key)
  content_length = obj.content_length

  for sindex in range(len(stats)):
    pipeline = stats[sindex]
    costs[sindex] = (pipeline[-2]["max_duration"] * memory["ec2"][params["ec2"]["type"]]) / (3600 * 1000)
    s3_costs[sindex] = (content_length / 1024 / 1024 / 1024) * 0.23
    if key in SSW_BYTES_WRITTEN:
      s3_costs[sindex] += (SSW_BYTES_WRITTEN[key] / 1024 / 1024 / 1024) * 0.23
    s3_costs[sindex] += (1.0 / 1000) * 0.0004
    s3_costs[sindex] += (1.0 / 1000) * 0.005

  return costs, s3_costs


def get_lambda_runtime_stats(key, stats, params):
  durations = list(map(lambda s: 0, stats))

  for sindex in range(len(stats)):
    pipeline = stats[sindex]
    start_time = None
    end_time = None
    for i in range(len(pipeline)):
      stat = pipeline[i]
      name = stat["name"]
      if name not in ["load", "total"]:
        for message in stat["messages"]:
          jmessage = json.loads(message)
          start = jmessage["start_time"]
          end = start + math.ceil(jmessage["duration"] / 1000)
          if start_time is None:
            start_time = start
            end_time = end
          else:
            start_time = min(start, start_time)
            end_time = max(end, end_time)
    durations[sindex] = end_time - start_time

  return durations


def get_lambda_cost_stats(key, stats, params):
  memory = json.loads(open("json/memory.json").read())
  s3_costs = list(map(lambda s: 0, stats))
  costs = list(map(lambda s: 0, stats))
  if params["tag"] == "smith-waterman":
    bucket = "ssw-input"
  else:
    bucket = "shjoyner-als" if key.startswith("TN_") else "shjoyner-ash"
  s3 = util.s3(params)
  obj = s3.Object(bucket, key)
  content_length = obj.content_length

  global SSW_BYTES_WRITTEN
  for sindex in range(len(stats)):
    pipeline = stats[sindex]
    s3_costs[sindex] += (content_length / 1024 / 1024 / 1024) * 0.23
    SSW_BYTES_WRITTEN[key] = 0
    for i in range(len(pipeline)):
      stat = pipeline[i]
      layer = i - 1
      name = stat["name"]
      if name not in ["load", "total"]:
        for message in stat["messages"]:
          jmessage = json.loads(message)
          function_name = params["pipeline"][layer]["name"]
          memory_size = str(params["functions"][function_name]["memory_size"])
          duration_cost = int(jmessage["duration"] / 100) * memory["lambda"][memory_size]
          costs[sindex] += duration_cost
          s3_costs[sindex] += (jmessage["read_count"] / 1000) * 0.0004
          s3_costs[sindex] += (jmessage["write_count"] / 1000) * 0.005
          s3_costs[sindex] += (jmessage["list_count"] / 1000) * 0.005
          if layer == 1 and "fasta" in key:
            SSW_BYTES_WRITTEN[key] += jmessage["write_byte_count"]
    if key in SSW_BYTES_WRITTEN:
      s3_costs[sindex] += (SSW_BYTES_WRITTEN[key] / 1024 / 1024 / 1024) * 0.023
  return [costs, s3_costs]


def process_ec2(application, ack):
  ec2_folder = "{0:s}-files-ec2".format(ack)
  total_s3_costs = []
  total_costs = []
  total_runtimes = []
  runtimes = {}
  s3_costs = {}
  costs = {}
  stats = {}
  params = json.load(open("json/ec2-{0:s}.json".format(application)))
  for subdir, dirs, files in os.walk("results/" + ec2_folder):
    if subdir.count("/") == 3:
      folder = subdir.split("/")[2]
      if folder not in stats:
        stats[folder] = []
      s = json.load(open(subdir + "/stats"))["stats"]
      stats[folder].append(s)

  for key in stats:
    runtimes[key] = get_ec2_runtime_stats(application, key, stats[key], params)
    total_runtimes += runtimes[key]
    c, s3 = get_ec2_cost_stats(application, key, stats[key], params)
    total_costs += c
    costs[key] = c
    total_s3_costs += s3
    s3_costs[key] = s3

  print("EC2 Average Runtime", numpy.average(total_runtimes))
  print("EC2 Average Cost", numpy.average(total_costs))
  print("EC2 Average S3 Cost", numpy.average(total_s3_costs))

  return costs, s3_costs, runtimes


def process_lambda(application, ack):
  lambda_folder = "{0:s}-files-lambda".format(ack)
  total_costs = []
  total_s3_costs = []
  total_runtimes = []
  costs = {}
  s3_costs = {}
  runtimes = {}
  stats = {}
  params = json.load(open("json/{0:s}.json".format(application)))
  for subdir, dirs, files in os.walk("results/" + lambda_folder):
    if subdir.count("/") == 3:
      folder = subdir.split("/")[2]
      if folder not in stats:
        stats[folder] = []
      s = json.load(open(subdir + "/stats"))["stats"]
      stats[folder].append(s)

  for key in stats:
    runtimes[key] = get_lambda_runtime_stats(key, stats[key], params)
    total_runtimes += runtimes[key]
    c, s3 = get_lambda_cost_stats(key, stats[key], params)
    total_costs += c
    total_s3_costs += s3
    costs[key] = c
    s3_costs[key] = s3

  print("Lambda Average Runtime", numpy.average(total_runtimes))
  print("Lambda Average Cost", numpy.average(total_costs))
  print("Lambda Average S3 Cost", numpy.average(total_s3_costs))

  return costs, s3_costs, runtimes


def render(title, application, ack, params):
  lambda_costs, lambda_s3_costs, lambda_runtimes = process_lambda(application, ack)
  ec2_costs, ec2_s3_costs, ec2_runtimes = process_ec2(application, ack)
  labels = ["Lambda Cost", "Lambda S3 Cost", "EC2 Cost", "EC2 S3 Cost"]
  costs = [lambda_costs, lambda_s3_costs, ec2_costs, ec2_s3_costs]
  plot.statistics(title, "results/{0:s}-files-lambda".format(ack), costs, labels, "cost")
  labels = ["Lambda Runtime", "EC2 Runtime"]
  runtimes = [lambda_runtimes, ec2_runtimes]
  plot.statistics(title, "results/{0:s}-files-lambda".format(ack), runtimes, labels, "duration")

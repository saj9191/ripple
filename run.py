import argparse
import benchmark
import boto3
import json
import math
import os
import plot
import re
import sys
import time
import util


class Results:
  def __init__(self, params):
    self.params = params


def clear():
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-tide")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-logs")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-ssw")
  bucket.objects.all().delete()


def get_counts(params):
  count_regex = re.compile("READ COUNT ([0-9]+) WRITE COUNT ([0-9]+) LIST COUNT ([0-9]+)")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-logs")
  layer_to_counts = {}
  READ_INDEX = 0
  WRITE_INDEX = 1
  LIST_INDEX = 2

  num_objs = 0
  for obj in bucket.objects.all():
    obj_format = util.parse_file_name(obj.key)
    layer = obj_format["prefix"]
    if layer not in layer_to_counts:
      layer_to_counts[layer] = [0, 0, 0]

    o = s3.Object("shjoyner-logs", obj.key)
    content = util.read(o, 0, o.content_length)
    m = count_regex.search(content)
    if m:
      for i in range(3):
        layer_to_counts[layer][i] += int(m.group(i + 1))
    else:
      print("Sad. Can't find count", obj.key)

    num_objs += 1
    if num_objs % 100 == 0:
      print("Processed {0:d} objects".format(num_objs))

  layers = sorted(layer_to_counts.keys())
  for layer in layers:
    print(params["pipeline"][layer - 1]["name"],
          "READ COUNT", layer_to_counts[layer][READ_INDEX],
          "WRITE COUNT", layer_to_counts[layer][WRITE_INDEX],
          "LIST COUNT", layer_to_counts[layer][LIST_INDEX]
          )


def trigger_plot(folder):
  results = []
  timestamp = time.time()
  params = json.loads(open("json/tide.json").read())
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      if params is None:
        params = json.loads(open("{0:s}/{1:s}/params".format(folder, d)).read())
      parts = d.split("-")
      now = float(parts[0])
      nonce = int(parts[1])
      p = dict(params)
      p["now"] = now
      p["nonce"] = nonce
      p["timestamp"] = timestamp
      results.append(Results(p))

  params["timestamp"] = timestamp
  params["folder"] = folder
  plot.plot(results, params["pipeline"], params)


def iterate(bucket_name, params):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  folder = "results/{0:s}".format(params["folder"])
  [access_key, secret_key] = util.get_credentials(params["ec2"]["key"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["stats"] = False
  params["sample_input"] = True
  params["setup"] = False

  objects = list(bucket.objects.all())[32:]
  data_bucket = s3.Bucket(params["bucket"])
  for obj in objects:
    params["input_name"] = obj.key
    [upload_duration, duration, failed_attempts] = benchmark.run(params, 0)
    dir_path = "{0:s}/{1:f}-{2:d}".format(folder, params["now"], params["nonce"])
    os.makedirs(dir_path)
    data_bucket.objects.all().delete()

    stats = benchmark.parse_logs(params, params["now"] * 1000, upload_duration, duration)
    with open("{0:s}/params".format(dir_path), "w+") as f:
      f.write(json.dumps(params, indent=4, sort_keys=True))

    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

    benchmark.clear_buckets(params)


def cost(folder, params):
  layers_to_cost = {}
  layers_to_count = {}
  for i in range(len(params["pipeline"])):
    layers_to_cost[str(i)] = 0
    layers_to_count[str(i)] = 0

  for subdir, dirs, files in os.walk(folder):
    if len(files) > 0:
      file_name = "{0:s}/deps".format(subdir)
      if not os.path.isfile(file_name):
        continue

      deps = json.load(open(file_name))
      for layer in deps["layers_to_cost"]:
        layers_to_cost[layer] += deps["layers_to_cost"][layer]

      for layer in deps["layers_to_count"]:
        layers_to_count[layer] += deps["layers_to_count"][layer]

  total = 0
  for i in range(len(params["pipeline"])):
    s = str(i)
    layers_to_cost[s] = float(layers_to_cost[s]) / layers_to_count[s]
    total += layers_to_cost[s]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--clear', action="store_true", help="Clear bucket files")
  parser.add_argument('--plot', type=str, help="Plot graph")
  parser.add_argument('--counts', action="store_true", help="Get read / write counts")
  parser.add_argument('--parameters', type=str, help="JSON parameter file to use")
  parser.add_argument('--iterate', type=str, help="Bucket to iterate through")
  parser.add_argument('--what', action="store_true", help="ugh")
  args = parser.parse_args()

#  params = json.load(open(args.parameters))
#  cost("results/shjoyner-als-lambda", params)
  if args.what:
    what()
  if args.clear:
    clear()
  if args.plot:
    trigger_plot(args.plot)
  if args.counts:
    params = json.load(open(args.parameters))
    get_counts(params)
  if args.iterate:
    params = json.load(open(args.parameters))
    iterate(args.iterate, params)


def get_lambda_results(folder, params, concurrency=None):
  stats = []
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats.append(s)

  memory = json.loads(open("json/memory.json").read())
  layers_to_duration = {}
  layers_to_cost = {}
  layers_to_count = {}
  layers_to_list_count = {}
  layers_to_read_count = {}
  layers_to_write_count = {}
  layers_to_warm_start = {}
  points = []
  regions = {}

  average_duration = 0
  for pipeline in stats:
    result_regions = {}
    start_time = None
    timestamp = None
    nonce = None
    duration = 0
    for i in range(len(pipeline)):
      stat = pipeline[i]
      layer = i - 1
      name = stat["name"]
      if name not in ["load", "total"]:
        if layer not in layers_to_duration:
          layers_to_duration[layer] = {"timestamp": 0, "duration": 0}
          layers_to_cost[layer] = 0
          layers_to_count[layer] = 0
          layers_to_write_count[layer] = 0
          layers_to_read_count[layer] = 0
          layers_to_list_count[layer] = 0
          layers_to_warm_start[layer] = 0
        if layer not in result_regions:
          result_regions[layer] = [sys.maxsize, 0]

        for message in stat["messages"]:
          jmessage = json.loads(message)
          if layer == 0:
            if start_time is None:
              start_time = jmessage["start_time"]
              timestamp = jmessage["timestamp"]
              nonce = jmessage["nonce"]
            else:
              start_time = min(start_time, jmessage["start_time"])
          assert(timestamp == jmessage["timestamp"])
          assert(nonce == jmessage["nonce"])
          start = jmessage["start_time"] - start_time
          if util.is_set(jmessage, "found"):
            layers_to_warm_start[layer] += 1
          assert(start >= 0)
          end = start + math.ceil(jmessage["duration"] / 1000)
          duration = max(duration, end)
          result_regions[layer][0] = min(result_regions[layer][0], start)
          result_regions[layer][1] = max(result_regions[layer][1], end)

          points.append([start, 1, layer])
          points.append([end, -1, layer])
          layers_to_duration[layer]["timestamp"] += jmessage["start_time"]
          layers_to_duration[layer]["duration"] += jmessage["duration"]
          function_name = params["pipeline"][layer]["name"]
          memory_size = str(params["functions"][function_name]["memory_size"])
          layers_to_cost[layer] += int(jmessage["duration"] / 100) * memory["lambda"][memory_size]
          layers_to_cost[layer] += (jmessage["read_count"] / 1000) * 0.0004
          layers_to_cost[layer] += (jmessage["write_count"] / 1000) * 0.005
          layers_to_cost[layer] += (jmessage["list_count"] / 1000) * 0.005
          layers_to_write_count[layer] += jmessage["write_count"]
          layers_to_read_count[layer] += jmessage["read_count"]
          layers_to_list_count[layer] += jmessage["list_count"]
          layers_to_cost[layer] += (jmessage["byte_count"] / 1024 / 1024 / 1024) * 0.023
          layers_to_count[layer] += 1

    for layer in result_regions.keys():
      if layer not in regions:
        regions[layer] = [0.0, 0.0]

      for i in range(2):
        regions[layer][i] += result_regions[layer][i]

    average_duration += duration

  for layer in regions:
    regions[layer][0] /= len(stats)
    regions[layer][1] /= len(stats)

  average_cost = 0
  print("durations", layers_to_duration)
  print("count", layers_to_count)
  for layer in layers_to_duration:
    layers_to_duration[layer]["duration"] /= (layers_to_count[layer] * 1000)
    layers_to_duration[layer]["timestamp"] /= layers_to_count[layer]
    layers_to_warm_start[layer] /= layers_to_count[layer]
    layers_to_cost[layer] /= len(stats)
    average_cost += layers_to_cost[layer]

  print("WARM START", layers_to_warm_start)

  points.sort()
  layer_to_count = {}
  layer_to_x = {}
  layer_to_y = {}
  for layer in layers_to_duration:
    layer_to_count[layer] = 0
    layer_to_x[layer] = []
    layer_to_y[layer] = []

  for point in points:
    layer = point[2]
    layer_to_count[layer] += point[1]
    layer_to_x[layer].append(point[0])
    count = 0
    for i in range(0, layer + 1):
      count += layer_to_count[i]
    if concurrency is None:
      count = float(count) / len(stats)
    layer_to_y[layer].append(count)

  average_duration /= len(stats)
  print(average_duration)
  return [average_duration, layers_to_cost, regions, layer_to_x, layer_to_y]


def get_ec2_results(folder):
  stats = []
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats.append(json.load(open(file_name))["stats"])

  layers_to_averages = {}
  layers_to_cost = {}
  layers_to_names = {}
  for stat in stats:
    layer = 0
    for s in stat:
      s = s[0]
      if "name" not in s:
        s["name"] = "termination"
      layers_to_names[layer] = s["name"]
      if s["name"] not in ["load", "total"]:
        if layer not in layers_to_averages:
          layers_to_averages[layer] = 0
          layers_to_cost[layer] = 0
        layers_to_averages[layer] += s["max_duration"]
        layers_to_cost[layer] += s["cost"]
        layer += 1

  average_cost = 0
  average_duration = 0
  for layer in layers_to_averages.keys():
    layers_to_averages[layer] /= (len(stats) * 1000)
    layers_to_cost[layer] /= len(stats)
    average_cost += layers_to_cost[layer]
    average_duration += layers_to_averages[layer]

  return [layers_to_averages, layers_to_cost]


def what():
  params = json.loads(open("json/dna-compression.json").read())
  lambda_folder = "results/compression100"
  ec2_folder = "results/methyl-ec2"
  render("Methyl DNA Compression", "methyl_dna_compression", lambda_folder, ec2_folder, params, compare=False, concurrency=100)

  return
  params = json.loads(open("json/tide.json").read())
  lambda_folder = "results/test-tide100"
  ec2_folder = "results/tide-ec2"
  render("Tide", "tide", lambda_folder, ec2_folder, params, concurrency=100)

  params = json.loads(open("json/tide.json").read())
  lambda_folder = "results/tide"
  ec2_folder = "results/tide-ec2"
  render("Tide", "tide", lambda_folder, ec2_folder, params, compare=True)

  lambda_folder = "results/tide100"
  render("Tide", "tide", lambda_folder, ec2_folder, params, concurrency=100)

  params = json.loads(open("json/dna-compression.json").read())
  lambda_folder = "results/methyl-lambda"
  ec2_folder = "results/methyl-ec2"
  render("Methyl DNA Compression", "methyl", lambda_folder, ec2_folder, params)
  return
  params = json.loads(open("json/smith-waterman.json").read())
  lambda_folder = "results/test-ssw-lambda"
  ec2_folder = "results/ssw-ec2"
  render("Smith Waterman", "ssw", lambda_folder, ec2_folder, params, compare=False, concurrency=None)

  params = json.loads(open("json/smith-waterman.json").read())
  lambda_folder = "results/small-ssw-concurrency100"
  ec2_folder = "results/ssw-ec2"
  render("Smith Waterman", "ssw", lambda_folder, ec2_folder, params, compare=False, concurrency=100)

  lambda_folder = "results/small-ssw-concurrency200"
  ec2_folder = "results/ssw-ec2"
  render("Smith Waterman", "ssw", lambda_folder, ec2_folder, params, compare=False, concurrency=200)

  lambda_folder = "results/small-ssw-lambda"
  ec2_folder = "results/ssw-ec2"
  render("Smith Waterman", "ssw", lambda_folder, ec2_folder, params, compare=False, concurrency=None)


def render(title, name, lambda_folder, ec2_folder, params, compare=True, concurrency=None):
  [lambda_duration, ssw_lambda_cost, ssw_regions, ssw_x, ssw_y] = get_lambda_results(lambda_folder, params, concurrency)
  lambda_cost = sum(ssw_lambda_cost.values())
  print("Lambda duration", lambda_duration)

  plot_name = "{name}_accumulation".format(name = name)
  if concurrency is not None:
    plot_name = "{name}_{concurrency}".format(name = plot_name, concurrency = concurrency)

  plot.accumulation_plot(
      ssw_x,
      ssw_y,
      ssw_regions,
      params["pipeline"],
      "{0:s} Average Lambda Processes".format(title),
      plot_name,
      lambda_folder
  )
  if not compare:
    return

  [ssw_ec2_duration, ssw_ec2_cost] = get_ec2_results(ec2_folder)
  ec2_duration = sum(ssw_ec2_duration.values())
  ec2_cost = sum(ssw_ec2_cost.values())

  plot.comparison(
      "{0:s}_runtime_comparison".format(name),
      "{0:s} Runtime Comparison".format(title),
      lambda_duration,
      ec2_duration,
      "Runtime (Seconds)",
      params
  )
  plot.comparison(
      "{0:s}_cost_comparison".format(name),
      "{0:s} Cost Comparison".format(title),
      lambda_cost,
      ec2_cost,
      "Cost ($)",
      params
  )
  return


def regularize():
  folder = "results/tide100"
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats = []
      for ss in s:
        if type(ss) == dict:
          assert("name" in ss)
          stats.append(ss)
        elif type(ss) == list:
          assert(len(ss) == 1)
          stats.append(ss[0])
      print("length", len(stats))
      assert(len(stats) == 10)
      if not os.path.isdir("results/test-tide100/{0:s}".format(d)):
        os.mkdir("results/test-tide100/{0:s}".format(d))
        with open("results/test-tide100/{0:s}/stats".format(d), "w+") as f:
          f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

if __name__ == "__main__":
  main()

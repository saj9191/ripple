import argparse
import benchmark
import boto3
import json
import os
import plot
import re
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
  print("timestamp", timestamp)
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
  folder = "results/{0:s}".format(bucket_name)
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["stats"] = True
  params["sample_input"] = True
  params["setup"] = False

  objects = list(bucket.objects.all())
  data_bucket = s3.Bucket(params["bucket"])
  for obj in objects:
    params["input_name"] = obj.key
    print("Processing file {0:s}".format(obj.key))
    [upload_duration, duration, failed_attempts] = benchmark.run(params, 0)
#    stats = benchmark.parse_logs(params, params["now"] * 1000, upload_duration, duration)
    dir_path = "{0:s}/{1:f}-{2:d}".format(folder, params["now"], params["nonce"])
    os.makedirs(dir_path)
    data_bucket.objects.all().delete()
    continue

    with open("{0:s}/params".format(dir_path), "w+") as f:
      f.write(json.dumps(params, indent=4, sort_keys=True))

    stats = json.loads(open("results/{0:f}-{1:d}/stats".format(params["now"], params["nonce"])).read())["stats"]
    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

    deps = benchmark.create_dependency_chain(stats, 1, params)
    with open("{0:s}/deps".format(dir_path), "w+") as f:
      f.write(json.dumps(deps, indent=4, sort_keys=True, default=benchmark.serialize))

    for key in deps["layers_to_cost"].keys():
      print("Layer", key, "Cost", deps["layers_to_cost"][key] / deps["layers_to_count"][key])

    #benchmark.clear_buckets(params)


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
      print(deps["layers_to_cost"])
      for layer in deps["layers_to_cost"]:
        layers_to_cost[layer] += deps["layers_to_cost"][layer]

      for layer in deps["layers_to_count"]:
        layers_to_count[layer] += deps["layers_to_count"][layer]

  total = 0
  for i in range(len(params["pipeline"])):
    s = str(i)
    layers_to_cost[s] = float(layers_to_cost[s]) / layers_to_count[s]
    total += layers_to_cost[s]
    print("Layer", i, "Cost", layers_to_cost[s])
  print("Total", total)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--clear', action="store_true", help="Clear bucket files")
  parser.add_argument('--plot', type=str, help="Plot graph")
  parser.add_argument('--counts', action="store_true", help="Get read / write counts")
  parser.add_argument('--parameters', type=str, help="JSON parameter file to use")
  parser.add_argument('--iterate', type=str, help="Bucket to iterate through")
  args = parser.parse_args()

#  params = json.load(open(args.parameters))
#  cost("results/shjoyner-als-lambda", params)
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


def get_lambda_results(folder, params):
  deps = []
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/deps".format(folder, d)
      deps.append(json.load(open(file_name)))

  layers_to_duration = { "0": 0 }
  layers_to_cost = None
  layers_to_count = None
  for dep in deps:
    start_key = list(filter(lambda k: k.startswith("0:"), dep.keys()))[0]
    start_time = -1 * dep[start_key]["timestamp"]
    end_keys = list(filter(lambda k: k.startswith("14:"), dep.keys()))
    end_time = max(list(map(lambda k: dep[k]["timestamp"] + dep[k]["duration"] / 1000.0, end_keys)))
    end_time += start_time
    layers_to_duration["0"] += end_time
    if layers_to_cost is None:
      layers_to_cost = dep["layers_to_cost"]
      layers_to_count = dep["layers_to_count"]
    else:
      for layer in dep["layers_to_cost"].keys():
        layers_to_cost[layer] += dep["layers_to_cost"][layer]
        layers_to_count[layer] += dep["layers_to_count"][layer]

  layers_to_duration["0"] /= len(deps)
  for layer in layers_to_count.keys():
    layers_to_cost[layer] /= len(deps)

  for i in (list(range(6)) + [15]):
    del layers_to_cost[str(i)]

  return [layers_to_duration, layers_to_cost]


def get_ec2_results(folder):
  stats = []
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats.append(json.load(open(file_name))["stats"])

  layers_to_duration = {}
  layers_to_cost = {}
  for stat in stats:
    layer = 0
    for s in stat:
      s = s[0]
      i = str(layer)
      if "name" not in s:
        s["name"] = "termination"
      if s["name"] not in ["load", "total"]:
        if i not in layers_to_duration:
          layers_to_duration[i] = 0
          layers_to_cost[i] = 0
        layers_to_duration[i] += (s["max_duration"] / 1000.0)
        layers_to_cost[i] += s["cost"]
        layer += 1

  for layer in layers_to_duration.keys():
    layers_to_duration[layer] /= len(stats)
    layers_to_cost[layer] /= len(stats)
  return [layers_to_duration, layers_to_cost]


def comparison():
  params = json.loads(open("json/tide.json").read())
#  lambda_duration, lambda_cost = get_lambda_results("results/shjoyner-als-lambda", params)
  ec2_duration, ec2_cost = get_ec2_results("results/ssw-ec2")
  print(ec2_duration)
  print(ec2_cost)
#  plot.comparison("tide_runtime_comparison", "Tide Runtime Comparison", lambda_duration, ec2_duration, "Runtime (Seconds)", params)
#  plot.comparison("tide_cost_comparison", "Tide Cost Comparison", lambda_cost, ec2_cost, "Cost (Dollars)", params)


if __name__ == "__main__":
  comparison()
#  main()

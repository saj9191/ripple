import argparse
import benchmark
import boto3
import json
import os
import plot
import re
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
  folder_timestamp = float(folder.split("/")[-1])
  params = json.loads(open("{0:s}/params.json".format(folder)).read())
  params["timestamp"] = folder_timestamp

  f = open("{0:s}/files".format(folder))
  lines = f.readlines()
  results = []
  for line in lines:
    line = line.strip()
    parts = line.split("-")
    timestamp = float(parts[0])
    nonce = int(parts[1])
    p = dict(params)
    p["now"] = timestamp
    p["nonce"] = nonce
    results.append(Results(p))
    stats = json.loads(open("{0:s}/{1:s}/stats".format(folder, line)).read())
    deps = benchmark.create_dependency_chain(stats["stats"], 1)
    with open("{0:s}/{1:s}/deps".format(folder, line), "w+") as f:
      json.dump(deps, f, default=benchmark.serialize, sort_keys=True, indent=4)

  plot.plot(results, params["pipeline"], params)


def iterate(bucket_name, params):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  folder = "results/{0:s}".format(bucket_name)
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
 # params["stats"] = False
  params["sample_input"] = True
  params["setup"] = False

  objects = list(bucket.objects.all())
  data_bucket = s3.Bucket(params["bucket"])
  for obj in objects:
    params["input_name"] = obj.key
    print("Processing file {0:s}".format(obj.key))
    [upload_duration, duration, failed_attempts] = benchmark.run(params, 0)
    stats = benchmark.parse_logs(params, params["now"] * 1000, upload_duration, duration)
    dir_path = "{0:s}/{1:f}-{2:d}".format(folder, params["now"], params["nonce"])
    os.makedirs(dir_path)

    with open("{0:s}/params".format(dir_path), "w+") as f:
      f.write(json.dumps(params, indent=4, sort_keys=True))

    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))

    deps = benchmark.create_dependency_chain(stats, 1, params)
    with open("{0:s}/deps".format(dir_path), "w+") as f:
      f.write(json.dumps(deps, indent=4, sort_keys=True, default=benchmark.serialize))

    for key in deps["layers_to_cost"].keys():
      print("Layer", key, "Cost", deps["layers_to_cost"][key] / deps["layers_to_count"][key])

    data_bucket.objects.all().delete()
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


if __name__ == "__main__":
  main()

import argparse
import benchmark
import boto3
import json
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
  count_regex = re.compile("READ COUNT ([0-9]+) WRITE COUNT ([0-9]+) LIST ([0-9]+)")
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
      layer_to_counts[layer] = [0, 0]

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


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--clear', action="store_true", help="Clear bucket files")
  parser.add_argument('--plot', type=str, help="Plot graph")
  parser.add_argument('--counts', action="store_true", help="Get read / write counts")
  parser.add_argument('--parameters', type=str, help="JSON parameter file to use")
  args = parser.parse_args()

  if args.clear:
    clear()
  if args.plot:
    trigger_plot(args.plot)
  if args.counts:
    params = json.load(open(args.parameters))
    get_counts(params)


if __name__ == "__main__":
  main()

import argparse
import boto3
import json
import os
from pathlib import Path
import setup
import sys
import util

def process_objects(s3, bucket_name, objects, params, subfolder):
  costs = {-1: 0}
  duration_cost = {-1: 0}
  durations = {-1: [sys.maxsize, 0]}
  list_count = {-1: 0}
  read_count = {-1: 0}
  write_count = {-1: 0}
  path = os.path.dirname(os.path.realpath(__file__))
  memory_parameters = json.loads(open(path + "/json/memory.json").read())
  statistics = []

  for stage in params["pipeline"]:
    statistics.append({"name": stage["name"], "messages": []})

  for objSum in objects:
    name = subfolder + "/" + objSum.key.replace("/", ".")
    obj_format = util.parse_file_name(objSum.key)
    if os.path.isfile(name):
      content = open(name, "r").read()
      if len(content.strip()) > 0:
        body = json.loads(open(name, "r").read())
      else:
        continue
    else:
      Path(name).touch()
      print("Not Found", name)
      obj = s3.Object(bucket_name, objSum.key)
      x = obj.get()["Body"].read()
      body = json.loads(x.decode("utf-8"))
      with open(name, "wb+") as f:
        f.write(x)
    duration = body["duration"]
    stage = obj_format["prefix"] - 1

    for prefix in [-1, stage]:
      if prefix not in costs:
        costs[prefix] = 0
        list_count[prefix] = 0
        write_count[prefix] = 0
        read_count[prefix] = 0
        duration_cost[prefix] = 0
        durations[prefix] = [sys.maxsize, 0]
      list_count[prefix] += body["list_count"]
      read_count[prefix] += body["read_count"]
      write_count[prefix] += body["write_count"]
      costs[prefix] += (body["write_count"] + body["list_count"]) / 1000.0 * 0.005
      costs[prefix] += body["read_count"] / 1000.0 * 0.0004
      memory_size = str(params["functions"][body["name"]]["memory_size"])
      costs[prefix] += memory_parameters["lambda"][memory_size] * int(float(duration + 99) / 100)
      duration_cost[prefix] += memory_parameters["lambda"][memory_size] * int(float(duration + 99) / 100)
      start_time = body["start_time"]
      end_time = start_time + body["duration"] / 1000.0

      for p in [-1, prefix]:
        durations[p][0] = min(durations[p][0], start_time)
        durations[p][1] = max(durations[p][1], end_time)

    statistics[stage]["messages"].append({"log": objSum.key, "body": body})

  print("Write count", write_count[-1])
  print("Read count", read_count[-1])
  print("List count", list_count[-1])
  print("Duration cost", duration_cost[-1])
  return [statistics, costs, durations]


def record(output, f):
  if f:
    f.write(output + "\n")
  else:
    print(output)


def statistics(bucket_name, token, prefix, params, output_folder):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  if prefix is None and token is None:
    objects = list(bucket.objects.all())
  elif prefix is None and token is not None:
    objects = list(bucket.objects.all())
    objects = list(filter(lambda o: token == o.key.split("/")[1], objects))
  elif prefix is not None and token is None:
    print("prefix", str(prefix) + "/")
    objects = list(bucket.objects.filter(Prefix=str(prefix) + "/"))
  else:
    objects = list(bucket.objects.filter(Prefix=str(prefix) + "/" + token))

  [statistics, costs, durations] = process_objects(s3, bucket_name, objects, params, output_folder)

  print("Total Cost " + str(costs[-1]))

  print("Total Duration " + str(durations[-1][1] - durations[-1][0]) + " seconds")

  if output_folder:
    with open(output_folder + "/statistics", "w+") as f:
      f.write(json.dumps({"stats": statistics}, indent=4, sort_keys=True))

  return [statistics, costs, durations]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket_name", type=str, required=True, help="Bucket to clear")
  parser.add_argument("--token", type=str, default=None, help="Only delete objects with the specified timestamp / nonce pair")
  parser.add_argument("--prefix", type=int, default=None, help="Only delete objects with the specified prefix")
  parser.add_argument("--parameters", type=str, help="JSON file containing application setup")
  parser.add_argument("--output_folder", type=str, help="Output folder to record statistics")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup.process_functions(params)
  statistics(args.bucket_name, args.token, args.prefix, params, args.output_folder)


if __name__ == "__main__":
  main()

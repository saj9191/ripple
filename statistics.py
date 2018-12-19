import argparse
import boto3
import json
import setup
import sys
import util


def process_objects(s3, bucket_name, objects, params):
  costs = {-1: 0}
  durations = {-1: [sys.maxsize, 0]}
  memory_parameters = json.loads(open("json/memory.json").read())
  statistics = []

  for stage in params["pipeline"]:
    statistics.append({"name": stage["name"], "messages": []})

  for objSum in objects:
    obj_format = util.parse_file_name(objSum.key)
    obj = s3.Object(bucket_name, objSum.key)
    body = json.loads(obj.get()["Body"].read().decode("utf-8"))
    duration = body["duration"]
    stage = obj_format["prefix"] - 1

    for prefix in [-1, stage]:
      if prefix not in costs:
        costs[prefix] = 0
        durations[prefix] = [sys.maxsize, 0]
      costs[prefix] += (body["write_count"] + body["list_count"]) / 1000.0 * 0.005
      costs[prefix] += body["read_count"] / 1000.0 * 0.0004
      memory_size = str(params["functions"][body["name"]]["memory_size"])
      costs[prefix] += memory_parameters["lambda"][memory_size] * int((duration + 99) / 100)
      start_time = body["start_time"]
      end_time = start_time + body["duration"] / 1000.0

      for p in [-1, prefix]:
        durations[p][0] = min(durations[p][0], start_time)
        durations[p][1] = max(durations[p][1], end_time)

    statistics[stage]["messages"].append(body)

  return [statistics, costs, durations]


def statistics(bucket_name, token, prefix, params, show=False):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  if prefix is None and token is None:
    objects = list(bucket.objects.all())
  elif prefix is None and token is not None:
    objects = list(bucket.objects.all())
    objects = list(filter(lambda o: token == o.key.split("/")[1], objects))
  elif prefix is not None and token is None:
    objects = list(bucket.objects.filter(Prefix=str(prefix)))
  else:
    objects = list(bucket.objects.filter(Prefix=str(prefix) + "/" + token))

  [statistics, costs, durations] = process_objects(s3, bucket_name, objects, params)

  print("Section Costs")
  for prefix in costs.keys():
    if prefix != -1:
        print(params["pipeline"][prefix]["name"], costs[prefix])

  print("Total Cost", costs[-1])
  print("Section Durations")
  for prefix in durations.keys():
    if prefix != -1:
      print(params["pipeline"][prefix]["name"], durations[prefix][1] - durations[prefix][0], "seconds")

  print("Total Duration", durations[-1][1] - durations[-1][0], "secionds")
  return [statistics, costs]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket_name", type=str, required=True, help="Bucket to clear")
  parser.add_argument("--token", type=str, default=None, help="Only delete objects with the specified timestamp / nonce pair")
  parser.add_argument("--prefix", type=int, default=None, help="Only delete objects with the specified prefix")
  parser.add_argument("--parameters", type=str, help="JSON file containing application setup")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup.process_functions(params)
  statistics(args.bucket_name, args.token, args.prefix, params, show=False)


if __name__ == "__main__":
  main()

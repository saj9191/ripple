import argparse
import boto3
import graph
import inspect
import json
import os
import sys
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import statistics


def download(subfolder, bucket_name, params):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  objSums = bucket.objects.filter(Prefix="1/")

  total_cost = 0.0
  average_duration = 0.0
  count = 0
  tokens = set()
  for objSum in objSums:
    token = objSum.key.split("/")[1]
    tokens.add(token)

  for token in tokens:
    print("token", token)
    dir_name = subfolder + "/" + token
    if not os.path.isdir(dir_name):
      os.mkdir(dir_name)
    [stats, costs, durations] = statistics.statistics(bucket_name, token, None, params, dir_name)
    total_cost += costs[-1]
    average_duration += durations[-1][1] - durations[-1][0]
    count += 1
  average_duration /= float(count)
  print("Average duration", average_duration)


def s3_cost(stats):
  cost = 0
  list_count = 0
  read_count = 0
  write_count = 0
  for message in stats["messages"]:
    list_count += message["list_count"]
    read_count += message["read_count"]
    write_count += message["write_count"]

  cost += (write_count + list_count) / 1000.0 * 0.005
  cost += read_count / 1000.0 * 0.0004
  return cost


def old_subprocess(name, params, lambda_ranges, task_ranges):
  results = json.loads(open(name, "r").read())
  st = results["durations"]["-1"][0]
  et = results["durations"]["-1"][1]
  functions = params["functions"]
  st = None
  et = None
  i = 0
  for stage in results["stats"]:
    st = sys.maxsize
    et = 0
    for j in range(len(stage["messages"])):
      message = stage["messages"][j]
      memory = functions[message["name"]]["memory_size"]
      vcpus = 1
      start_time = message["start_time"]
      end_time = start_time + message["duration"] / 1000.0
      st = min(start_time, st)
      et = max(end_time, et)
      if i == 2:
        lambda_ranges.append([start_time, vcpus])
        lambda_ranges.append([end_time, -1 * vcpus])
      if st:
        st = min(st, start_time)
        et = max(et, end_time)
      else:
        st = start_time
        et = end_time
    i += 1
  print("WTF", name, st)
  return [st, et]


def subprocess(name, params, lambda_ranges, task_ranges, cpus):
  message = json.loads(open(name).read())
  functions = params["functions"]
  #pipeline = params["pipeline"]
  #for stage in range(len(results["stats"])):
  #  for message in results["stats"][stage]["messages"]:
  start_time = message["start_time"]
  cpus += message["cpu"]
  end_time = start_time + message["duration"] / 1000.0
  memory = functions[message["name"]]["memory_size"]
  vcpus = (2 * memory) / 3008
  lambda_ranges.append([start_time, vcpus])
  lambda_ranges.append([end_time, -1*vcpus])
  return [start_time, end_time]
#  durations = results["durations"]
#  task_ranges.append([durations["-1"][0], 1])
#  task_ranges.append([durations["-1"][1], -1])


def process(subfolder, params, old):
  lambda_ranges = []
  task_ranges = []
  count = 0
  total_duration = 0
  cpu_averages = {}
  for d in os.listdir(subfolder):
    if d.endswith(".png"):
      continue
    if old:
      name = subfolder + "/" + d
      t = float(name.split("/")[-1].split("-")[0])
      if name.endswith(".png") or name.endswith("README") or name.endswith("statistics") or name.endswith(".swp"):
        continue
      [start_time, end_time] = old_subprocess(name, params, lambda_ranges, task_ranges)
      print(t, start_time, start_time - t)
    else:
      start_time = None
      end_time = None
      for f in os.listdir(subfolder + "/" + d):
        name = subfolder + "/" + d + "/" + f
        if name.endswith(".png") or name.endswith("README") or name.endswith("statistics") or name.endswith(".swp"):
          continue
        stage = int(f.split(".")[0])
        if stage not in cpu_averages:
          cpu_averages[stage] = []
        print(f)
        [st, et] = subprocess(name, params, lambda_ranges, task_ranges, cpu_averages[stage])
        if start_time:
          start_time = min(st, start_time)
          end_time = max(et, end_time)
        else:
          start_time = st
          end_time = et
    duration = end_time - start_time
    total_duration += duration
    task_ranges.append([start_time, 1])
    task_ranges.append([end_time, -1])
    count += 1

  average_duration = float(total_duration) / count
  print("Average Duration", average_duration)
  start_time = min(list(map(lambda t: t[0], task_ranges)))
  ranges = [lambda_ranges, task_ranges]
  results = list(map(lambda r: [], ranges))
  print("Num tasks", len(lambda_ranges))
  for i in range(len(ranges)):
    ranges[i] = sorted(ranges[i], key=lambda r: r[0])
    ranges[i] = list(map(lambda r: [r[0] - start_time, r[1]], ranges[i]))
    mcount = 0
    count = 0
    num = 0
    for j in range(len(ranges[i])):
      [timestamp, increment] = ranges[i][j]
      results[i].append([timestamp - 1, num])
      if increment < 0:
        count -= 1
      else:
        count += 1
      mcount = max(count, mcount)
      num += increment
      results[i].append([timestamp, num])

    print("Count", mcount) 
  return results


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket", type=str, required=True, help="Log bucket")
  parser.add_argument("--subfolder", type=str, required=True, help="Subfolder to download results in")
  parser.add_argument("--parameters", type=str, required=True, help="Location of JSON parameter file")
  parser.add_argument("--start_range", type=int, help="Start timestamp of zoom region")
  parser.add_argument("--end_range", type=int, help="End timestamp of zoom region")
  parser.add_argument("--download", default=False, action="store_true", help="Download the data")
  parser.add_argument("--old", default=False, action="store_true", help="Old save format")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  if args.download:
    download(args.subfolder, args.bucket, params)
  numbers = process(args.subfolder, params, args.old)
  colors = ["red", "blue"]
  labels = ["Number of VCPUs", "Number of Running Jobs"]
  graph.graph(args.subfolder, numbers, colors, labels, args.start_range, args.end_range)

if __name__ == "__main__":
  main()

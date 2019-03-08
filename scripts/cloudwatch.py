import argparse
import boto3
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import re
import sys
import time


MEMORY_REGEX = re.compile(".*Duration:\s([0-9\.]+).*Billed Duration.*Memory Size: ([0-9]+).*Max Memory Used: ([0-9]+).*")


def parse(logs):
  results = []
  for event in logs:
    end_time = event["timestamp"] / 1000.0
    m = MEMORY_REGEX.match(event["message"])
    assert(m is not None)
    duration = float(m.group(1))
    used = int(m.group(3))
    total = int(m.group(2))
    start_time = end_time - (duration / 1000.0)
    results.append((start_time, end_time, used, total))
  return results


def get_events(client, name, start_time, end_time, pattern):
  response = client.filter_log_events(
    logGroupName=name,
    startTime=start_time,
    endTime=end_time,
    filterPattern=pattern
  )
  events = response["events"]
  while "nextToken" in response:
    response = client.filter_log_events(
      logGroupName=name,
      startTime=start_time,
      endTime=end_time,
      nextToken=response["nextToken"],
      filterPattern=pattern
    )
    events += response["events"]
    nextToken = None
    if "nextToken" in response:
      nextToken = response["nextToken"]
    print(name, "Num events", len(events), nextToken)
  print("Num events", len(events))
  return events


def pywren_logs(client, pattern, start_time, end_time):
  return get_events(client, "/aws/lambda/pywren_1", start_time, end_time, pattern)


def ripple_logs(client, pattern, params, start_time, end_time):
  events = []

  for function in params["functions"].keys():
    name = "/aws/lambda/{0:s}".format(function)
    events += get_events(client, name, start_time, end_time, pattern)
  return events


def fetch_logs(params, start_time, end_time):
  if params:
    session = boto3.Session(region_name=params["region"])
  else:
    session = boto3.Session(region_name="us-west-1")

  client = session.client("logs")
  pattern = "Max Memory Used"
  if params:
    return ripple_logs(client, pattern, params, start_time, end_time)
  else:
    return pywren_logs(client, pattern, start_time, end_time)

def statistics(results):
  total_memory_used = 0
  total_memory = 0
  for result in results:
    total_memory_used += result[2]
    total_memory += result[3]
  print("Percent memory used", float(total_memory_used) / total_memory)


def compare(subfolder, ripple_results, pywren_results):
  ripple_used_results = cdf(subfolder, ripple_results, 2)
  pywren_used_results = cdf(subfolder, pywren_results, 2)
  results = [ripple_used_results, pywren_used_results]
  labels = ["Ripple Mem Used", "PyWren Mem Used"]
  plot("compare", subfolder, results, labels)


def cdf(subfolder, results, index):
  counts = {}
  for i in range(len(results)):
    result = results[i]
    mem = result[index]
    if mem not in counts:
      counts[mem] = 0
    counts[mem] += 1

  X = np.array(list(counts.keys()))
  print("Min memory", min(X))
  print("Max memory", max(X))
  X.sort()
  Y = np.zeros(len(X))

  for i in range(len(X)):
    Y[i] = counts[X[i]]

  print("Max Memory", results[0][3])
  Y /= Y.sum()
  CY = np.cumsum(Y)
  return [X, CY]


def plot(name, subfolder, results, labels):
  fig, ax = plt.subplots()
  min_x = sys.maxsize
  max_x = 0
  for i in range(len(results)):
    [X, CY] = results[i]
    ax.plot(X, CY, label=labels[i])
    min_x = min(min(X), min_x)
    max_x = max(max(X), max_x)
  plt.xlim([min_x, max_x])
  plt.ylabel("CDF")
  plt.xlabel("Max Memory Used (MB)")
  plot_name = subfolder + "/" + name + ".png"
  print("Plot_name", plot_name)
  ax.legend(labels)
  fig.savefig(plot_name)
  plt.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--compare", type=str)
  parser.add_argument("--duration", type=int)
  parser.add_argument("--subfolder", type=str, required=True)
  parser.add_argument("--load", action="store_true")
  parser.add_argument("--parameters", type=str)
  args = parser.parse_args()

  if args.compare:
    print("Ripple")
    ripple_results = parse(json.load(open(args.subfolder + "/logs", "r"))["logs"])
    print("PyWren")
    pywren_results = parse(json.load(open(args.compare + "/logs", "r"))["logs"])
    compare(args.subfolder, ripple_results, pywren_results)
  else:
    end_time = int(time.time()) * 1000
    start_time = end_time - args.duration * 1000
    params = None
    if args.parameters:
      params = json.load(open(args.parameters, "r"))
    if args.load:
      logs = json.load(open(args.subfolder + "/logs", "r"))["logs"]
    else:
      logs = fetch_logs(params, start_time, end_time)
      with open(args.subfolder + "/logs", "w+") as f:
        f.write(json.dumps({"logs": logs}))
    results = parse(logs)
    statistics(results)
    cdf(args.subfolder, results)

main()

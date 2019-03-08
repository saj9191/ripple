import argparse
import boto3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import re
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


def fetch_logs(start_time, end_time):
  session = boto3.Session(region_name="us-west-1")
  client = session.client("logs")
  pattern = "Max Memory Used"
  
  response = client.filter_log_events(
    logGroupName="/aws/lambda/pywren_1",
    startTime=start_time,
    endTime=end_time,
    filterPattern=pattern
  )
  events = response["events"]
  while "nextToken" in response:
    response = client.filter_log_events(
      logGroupName="/aws/lambda/pywren_1",
      startTime=start_time,
      endTime=end_time,
      nextToken=response["nextToken"],
      filterPattern=pattern
    )
    events += response["events"]
    print("Num events", len(events))
  print("Num events", len(events))
  return events


def statistics(results):
  total_memory_used = 0
  total_memory = 0
  for result in results:
    total_memory_used += result[2]
    total_memory += result[3]
  print("Percent memory used", float(total_memory_used) / total_memory)


def cdf(results):
#  X.sort()
  
  counts = {}
  for i in range(len(results)):
    result = results[i]
    mem = float(result[2])
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
#  X /= float(results[0][3])
  print("X", X)
  print("Y", Y)
#  X *= 100
  Y /= Y.sum()
  CY = np.cumsum(Y)

  fig, _ = plt.subplots()
  plt.plot(X, CY)
  plt.xlim([min(X), max(X)])
  plt.ylabel("CDF")
  plt.xlabel("Max Memory Used (MB)")
  fig.savefig("CDF.png")
  plt.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--duration", type=int, required=True)
  args = parser.parse_args()

  end_time = int(time.time()) * 1000
  start_time = end_time - args.duration * 1000
  logs = fetch_logs(start_time, end_time)
  results = parse(logs)
  statistics(results)
  cdf(results)

main()

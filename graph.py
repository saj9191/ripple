import argparse
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import sys

memory = json.loads(open("json/memory.json").read())

def cumulative(ranges, min_time):
  keys = list(map(lambda key: [key - min_time, ranges[key]], ranges.keys()))
  keys.sort()
  cum = [[0, 0]]
  count = 0
  for [timestamp, c] in keys:
    count +=  c
    cum.append([timestamp, count])
  return cum


def walk(folder):
  ranges = {}
  min_time = sys.maxsize
  print(folder)
  count = 0

  tokens = {}
  payloads = set()
  times = {}
  invoke = {}
  not_invoke = {}

  for _, _, files in os.walk(folder):
    for file_name in files:
      if file_name not in ["statistics", "README"]:
        with open(folder + "/" + file_name) as f:
          count += 1
          parts = file_name.split(".")
          token = ".".join(parts[1:3])
          if token not in tokens:
            tokens[token] = len(tokens)
          parts = ".".join(parts[0:1] + [str(tokens[token])] + parts[3:])
          stats = json.loads(f.read())
          payloads.add(parts)
          start_time = stats["start_time"]
          min_time = min(start_time, min_time)
          end_time = start_time + (stats["duration"] / 1000.0)
          times[parts] = [start_time, end_time]
          if end_time not in ranges:
            ranges[end_time] = 0
            invoke[end_time] = 0
          if "invoke" in stats:
            invoke[end_time] += 1
          else:
            if end_time not in  not_invoke:
              not_invoke[end_time] = 0
            not_invoke[end_time] += 1
          ranges[end_time] += 1

  keys = list(map(lambda key: [key - min_time, ranges[key]], ranges.keys()))
  cum = cumulative(ranges, min_time)
  invoke = cumulative(invoke, min_time)
  not_invoke = cumulative(not_invoke, min_time)

  return cum, invoke, not_invoke#payloads, times, min_time


def what():
  print("Num files", count)
  return ranges

def parse_logs(folder, params):
  ranges = {}
  n = 1 
  for i in range(n):
    r = walk(folder)# + "-" + str(i+1))
    for key in r.keys():
      if key not in ranges:
        ranges[key] = []
      ranges[key] += r[key]

  colors = ['#ff3300', '#003300', '#883300', 'cyan', '#000000']
  marks = ["-", ":", "-.", "--", ]
  keys = list(ranges.keys())
  keys.sort()
  x_max = 0
  fig, ax = plt.subplots()
  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
  for i in range(len(keys)):
    name = keys[i] 
    print(name)
    ranges[name].sort()
    count = 0
    x = []
    y = []
    for r in ranges[name]:
      count += r[1]
      x.append(r[0])
      y.append(int(count / n))
    x_max = max(x_max, max(x))
    plt.plot(x, y, label=name, color=colors[i %  len(colors)], linestyle=marks[i % 4])
  plt.ylim([0, 1000])
  plt.legend(frameon=False, loc="upper right")#, ncol=3)
  plt.xlabel("Runtime (Seconds)")
  plt.ylabel("Number of Concurrent Functions")
  plot_name = "concurrency.png"
  plt.savefig(plot_name)
  plt.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str)
  parser.add_argument("--parameters", type=str)
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  #parse_logs(args.folder, params)
  normal, _, _ = walk("results/compression20-3")

  normal.append([5*60, normal[-1][1]])
  cum, invoke, not_invoke = walk("results/compression20-invoke-4")
  print("Invoke", len(invoke))
  print("NonInvoke", len(not_invoke))
  fig, ax = plt.subplots()
  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
  plt.ylim([0, 3500])
  normal_x = list(map(lambda x: x[0], normal))
  normal_y = list(map(lambda x: x[1], normal))
  plt.plot(normal_x, normal_y, label="No Fault Tolerance", color="red", linestyle="-")

  cum_x = list(map(lambda x: x[0], cum))
  cum_y = list(map(lambda x: x[1], cum))
  plt.plot(cum_x, cum_y, label="Fault Tolerance", color="blue", linestyle="-.")

  plt.xlabel("Runtime (Seconds)")
  plt.ylabel("Number of Cumulative Finished Functions")

  plot_name = "concurrency.png"
  plt.legend(frameon=False, loc="upper right", fontsize="large")
  plt.savefig(plot_name)
  plt.close()

  fig, ax = plt.subplots()
  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
  invoke_x = list(map(lambda x: x[0], invoke))
  invoke_y = list(map(lambda x: x[1], invoke))
  plt.ylim([0, 3500])
  plt.xlabel("Runtime (Seconds)")
  plt.ylabel("Number of Cumulative Finished Functions")

  plt.plot(invoke_x, invoke_y, label="Re-Spawned Tasks", color="brown", linestyle="-.")

  not_invoke_x = list(map(lambda x: x[0], not_invoke))
  not_invoke_y = list(map(lambda x: x[1], not_invoke))
  print("Las non invoke", not_invoke_x[-1])
  plt.plot(not_invoke_x, not_invoke_y, label="Non-Straggler Tasks", color="green", linestyle="--")
  plot_name = "concurrency-1.png"
  plt.legend(frameon=False, loc="upper right", fontsize="large")
  plt.savefig(plot_name)
  plt.close()


main()

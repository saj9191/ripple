import argparse
import inspect
import json
import matplotlib
import os
import sys
matplotlib.use('Agg')
import matplotlib.pyplot as plt
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/ec2")

def graph(subfolder, numbers, colors, labels=None, start_range=None, end_range=None):
  grid = plt.GridSpec(10, 1)
  min_timestamp = None
  max_timestamp = None
  max_concurrency = None
  handles = []
  linestyle = [":", "-", "-.", "--"]
  fig, ax = plt.subplots()
  for i in range(len(numbers)):
    num = numbers[i]
    timestamps = list(map(lambda r: r[0], num))
    min_t = min(timestamps)
    max_t = max(timestamps)
    total = list(map(lambda r: r[1], num))
    max_c = max(total)
    if min_timestamp:
      min_timestamp = min(min_timestamp, min_t)
      max_timestamp = max(max_timestamp, max_t)
      max_concurrency = max(max_concurrency, max_c)
    else:
      min_timestamp = min_t
      max_timestamp = max_t
      max_concurrency = max_c
    if labels:
      handles.append(plt.plot(timestamps, total, color=colors[i % len(colors)], label=labels[i], linestyle=linestyle[i]))
    else:
      handles.append(plt.plot(timestamps, total, color=colors[i % len(colors)]))#, linestyle=linestyle[i], linewidth=2*(i + 1)))

  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
  plt.xlabel("Time (Seconds)")
  plt.ylabel("Number of Concurrent Functions")
  plot_name = subfolder + "/simulation.png"
  plt.subplots_adjust(hspace=0.5)
  if labels:
    plt.legend(frameon=False, fontsize="large")
  print("Plot", plot_name)
  plt.savefig(plot_name)
  plt.close()



def process(subfolder):
  token_to_counts = {}
  min_start_time = None
  stage_to_token_to_range = {}
  stage_to_token_to_count = {}
  stage_to_token_to_duration = {}
  for f in os.listdir(subfolder):
    if f.endswith(".log"):
      try:
        body = json.loads(open(subfolder + "/" + f, "r").read())
      except Exception as e:
        continue
      start_time = body["start_time"]
      min_start_time = min([start_time, min_start_time]) if min_start_time else start_time

  for f in os.listdir(subfolder):
    if f.endswith(".log"):
      try:
        body = json.loads(open(subfolder + "/" + f, "r").read())
      except Exception as e:
        continue
      parts = f.split(".")
      stage = int(parts[0])
      if stage not in stage_to_token_to_range:
        stage_to_token_to_range[stage] = {}
        stage_to_token_to_count[stage] = {}
        stage_to_token_to_duration[stage] = {}
      token = ".".join(parts[1:3])
      if token not in stage_to_token_to_range[stage]:
        stage_to_token_to_range[stage][token] = [sys.maxsize, 0]
        stage_to_token_to_count[stage][token] = 0
        stage_to_token_to_duration[stage][token] = 0
      if token not in token_to_counts:
        token_to_counts[token] = []
      start_time = body["start_time"] - min_start_time
      assert(start_time >= 0)
      end_time = start_time + body["duration"] / 1000.0 - 1
      stage_to_token_to_range[stage][token][0] = min(stage_to_token_to_range[stage][token][0], start_time)
      stage_to_token_to_range[stage][token][1] = max(stage_to_token_to_range[stage][token][1], end_time)
      stage_to_token_to_count[stage][token] += 1
      stage_to_token_to_duration[stage][token] += (end_time - start_time)
      token_to_counts[token].append([start_time, 1])
      token_to_counts[token].append([end_time, -1])

  numbers = []
  for token in token_to_counts.keys():
    counts = token_to_counts[token]
    counts = sorted(counts, key=lambda c: c[0])
    ranges = []
    num_functions = 0
    max_num = 0
    for i in range(len(counts)):
      ranges.append([counts[i][0] - 1, num_functions])
      num_functions += counts[i][1]
      max_num = max(num_functions, max_num)
      ranges.append([counts[i][0], num_functions])
    numbers.append(ranges)

  colors = ['#ff3300', '#883300', '#000000']
  labels = ["Job 1", "Job 2", "Job 3"]
#          '#ff0044', '#00ffff', '#000000']
  graph(subfolder, numbers, colors, labels, None, None)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--subfolder", type=str, required=True, help="Subfolder with simulation data")
  args = parser.parse_args()
  process(args.subfolder)


main()

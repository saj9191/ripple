import argparse
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import sys

memory = json.loads(open("json/memory.json").read())

def walk(folder):
  print("walk")
  ranges = {}
  ranges = {"total": []}
  min_time = sys.maxsize
  print(folder)
  count = 0
  for _, _, files in os.walk(folder):
    for file_name in files:
      if file_name not in ["statistics", "README"]:
        with open(folder + "/" + file_name) as f:
          count += 1
          parts = file_name.split(".")
          stage = int(parts[0])
          token = ".".join(parts[1:3])
          stats = json.loads(f.read())
          if stats["name"] not in ranges:
            ranges[stats["name"]] = []
          start_time = stats["start_time"]
          min_time = min(start_time, min_time)
          end_time = start_time + (stats["duration"] / 1000.0)
          for x in [stats["name"], "total"]:
            ranges[x].append([start_time, 1])
            ranges[x].append([end_time, -1])

  print("Num files", count)
  for key in ranges:
    ranges[key] = list(map(lambda r: [r[0] - min_time, r[1]], ranges[key]))
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
    plt.plot(x, y, label=name, color=colors[i], linestyle=marks[i % 4])
  plt.ylim([0, 1000])
  plt.xlim([0, 600])
  plt.legend(frameon=False, loc="upper right")#, ncol=3)
  plt.xlabel("Runtime (Seconds)")
  plt.ylabel("Number of Concurrent Functions")
  plot_name = "concurrency.png"
  print("Max x", x_max)
  print("Plot", plot_name)
  plt.savefig(plot_name)
  plt.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str)
  parser.add_argument("--parameters", type=str)
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  parse_logs(args.folder, params)


main()

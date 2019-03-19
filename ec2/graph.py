import argparse
import json
import matplotlib
import os
import re
matplotlib.use('Agg')
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


S3_REGEX = re.compile("S3 CREATED TIME: ([0-9\.]+)")
TASK_START_REGEX = re.compile("EXECUTION START TIME: ([0-9\.]+)")
TASK_END_REGEX = re.compile("EXECUTION END TIME: ([0-9\.]+)")
NODE_START_REGEX = re.compile("NODE START TIME: ([0-9\.]+)")
NODE_END_REGEX = re.compile("NODE END TIME: ([0-9\.]+)")


def graph(subfolder, numbers, colors, pending_tasks, labels=None, start_range=None, end_range=None):
  grid = plt.GridSpec(10, 1)
  fig, ax = plt.subplots()
  ax.set_xticks([])
  ax.set_yticks([])
  ax.spines["right"].set_visible(False)
  ax.spines["top"].set_visible(False)
#  ax.yaxis.set_major_locator(MaxNLocator(integer=True))
  ax1 = fig.add_subplot(grid[:8, 0])
  ax1.spines["right"].set_visible(False)
  ax1.spines["top"].set_visible(False)
  min_timestamp = None
  max_timestamp = None
  max_concurrency = None
  linestyle = [":", "-", "-.", "--"]
  for i in range(len(numbers)):
    num = numbers[i]
    timestamps = list(map(lambda r: r[0], num))
    min_t = min(timestamps)
    max_t = max(timestamps)
    total = list(map(lambda r: int(r[1]), num))
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
      plt.plot(timestamps, total, color=colors[i % len(colors)], linestyle=linestyle[i])
    else:
      plt.plot(timestamps, total, color=colors[i % len(colors)])
  print("max_timestamp", max_timestamp)
  colors.append('#ffcc66')
  print(colors)
  if labels:
    labels.append("Number of Pending Jobs")
    handles = []
    for i in range(len(labels)):
      label = labels[i]
      handles.append(Line2D([0], [0], color=colors[i], linestyle=linestyle[i]))
    plt.legend(loc="upper right", frameon=False, handles=handles,  labels=labels, bbox_to_anchor=(1.10, 1.20))
  max_timestamp = 13315.110265016556
  yticks = range(0, 250, 50)
  top_y = 250#max_concurrency * 1.05
  bottom_y = 120
  plt.xticks([])
  plt.yticks(yticks)
  plt.ylim([0, top_y])
#  plt.ylim([0, max_concurrency * 1.05])
  plt.xlim([min_timestamp, max_timestamp])
  print("max", max_timestamp)
  ax2 = fig.add_subplot(grid[8:, 0])
  ax2.spines["top"].set_visible(False)
  ax2.spines["right"].set_visible(False)
  timestamps = list(map(lambda r: r[0], pending_tasks))
  total = list(map(lambda r: r[1], pending_tasks))
  plt.plot(timestamps, total, color='#ffcc66', linestyle="--")
#  if labels:
#    fig.legend(frameon=False, loc="upper right", bbox_to_anchor=(0.93, 0.95))

  plt.xlabel("Time (Seconds)")
  plt.ylim([0, bottom_y])
  if start_range:
    min_timestamp = start_range
  if end_range:
    max_timestamp = end_range

  plt.xlim([min_timestamp, max_timestamp])
  print(max_concurrency)
  plot_name = subfolder + "/simulation.png"
  plt.subplots_adjust(hspace=0.5)
  print("Plot", plot_name)
  plt.savefig(plot_name)
  plt.close()


def process_data(file, regex):
  content = open(file).read()
  results = list(map(lambda r: float(r.search(content).group(1)), regex))
  return results


def format_data(data, start_time):
  results = []
  for i in range(len(data)):
    start = data[i - 1] if i > 0 else start_time
    end = data[i]
    results.append(end - start)

  return results


def process_tasks(start_time, subfolder):
  results = []
  folder = subfolder + "/tasks/"
  regex = [S3_REGEX, TASK_START_REGEX, TASK_END_REGEX]
  files = os.listdir(folder)
  for file in files:
    results.append(process_data(folder + file, regex))

  total_ranges = []
  active_ranges = []
  pending_ranges = []
  duration = 0.0
  for i in range(len(results)):
    duration += (results[i][2] - results[i][0])
    total_ranges.append([results[i][0] - start_time, 1])
    total_ranges.append([results[i][2] - start_time, -1])
    pending_ranges.append([results[i][0] - start_time, 1])
    pending_ranges.append([results[i][1] - start_time, -1])
    active_ranges.append([results[i][1] - start_time, 1])
    active_ranges.append([results[i][2] - start_time, -1])

  print("Average task time", duration / len(results))
  ranges = [active_ranges, pending_ranges, total_ranges]
  for i in range(len(ranges)):
    ranges[i] = sorted(ranges[i], key=lambda r: r[0])

  tasks = list(map(lambda r: [], ranges))
  for i in range(len(ranges)):
    num = 0
    for j in range(len(ranges[i])):
      [timestamp, increment] = ranges[i][j]
      tasks[i].append([timestamp - 1, num])
      num += increment
      tasks[i].append([timestamp, num])

  return tasks


def num_vcpu(instance):
  if instance == "t2.xlarge":
    return 4
  elif instance == "r5a.xlarge":
    return 4
  raise Exception("num_vcpu", instance, "not implemented")

def process_nodes(subfolder, parameters):
  params = json.loads(open(subfolder + "/" + parameters, "r").read())
  vcpu = num_vcpu(params["instance"])
  node_times = []
  folder = subfolder + "/nodes/"
  regex = [NODE_START_REGEX, NODE_END_REGEX]
  files = os.listdir(folder)
  cost = 0.0
  for file in files:
    node_times.append(process_data(folder + file, regex))

  start_time = min(list(map(lambda r: r[0], node_times)))
  ranges = []
  read_count = 0
  write_count = 0
  volume_cost = 0
  for i in range(len(node_times)):
    st = node_times[i][0] - start_time
    et = node_times[i][1] - start_time
    cost += (et - st) * (0.1856 / 60 / 60)
    volume_cost += (et - st) * ((0.10 * 26) / 30 / 24 / 60 / 60)
    ranges.append([st, vcpu])
    ranges.append([et, -1*vcpu])
    read_count += 1
    write_count += 1


  ranges = sorted(ranges, key=lambda r: r[0])
  results = []
  num_nodes = 0
  for i in range(len(ranges)):
    results.append([ranges[i][0] - 1, num_nodes])
    num_nodes += ranges[i][1]
    results.append([ranges[i][0], num_nodes])

  s3_cost = (read_count / 1000.0 * 0.0004) + (write_count / 1000.0 * 0.005)
  cost += volume_cost
  cost += s3_cost
  print("Volume Cost", volume_cost)
  print("S3 Cost", s3_cost)
  print("Total cost", cost)
  print("Average cost per node", cost / len(node_times))
  return [start_time, results]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="Parameters to use")
  parser.add_argument("--subfolder", type=str, required=True, help="Folder containing data to graph")
  parser.add_argument("--start_range", type=int, help="Start timestamp of zoom region")
  parser.add_argument("--end_range", type=int, help="End timestamp of zoom region")
  args = parser.parse_args()
  [start_time, num_nodes] = process_nodes(args.subfolder, args.parameters)
  [active_tasks, pending_tasks, total_tasks] = process_tasks(start_time, args.subfolder)

  numbers = [num_nodes, active_tasks, total_tasks]
  #colors = ["red", "blue", "purple"]
  colors = ['#003300', '#ff3300', '#883300']
#  colors = ['#003300', '#ff3300']
  #labels = ["Number of VCPUs", "Number of Running Jobs", "Number of Total Jobs"]
  labels=None
  graph(args.subfolder, numbers, colors, pending_tasks, labels, args.start_range, args.end_range)


if __name__ == "__main__":
  main()

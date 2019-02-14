import argparse
import matplotlib
import os
import re
matplotlib.use('Agg')
import matplotlib.pyplot as plt


S3_REGEX = re.compile("S3 CREATED TIME: ([0-9\.]+)")
TASK_START_REGEX = re.compile("EXECUTION START TIME: ([0-9\.]+)")
TASK_END_REGEX = re.compile("EXECUTION END TIME: ([0-9\.]+)")
NODE_START_REGEX = re.compile("NODE START TIME: ([0-9\.]+)")
NODE_END_REGEX = re.compile("NODE END TIME: ([0-9\.]+)")


def graph(subfolder, numbers, colors, labels):
  fig, ax = plt.subplots()
  for i in range(len(numbers)):
    num = numbers[i]
    timestamps = list(map(lambda r: r[0], num))
    total = list(map(lambda r: r[1], num))
    plt.plot(timestamps, total, color=colors[i], label=labels[i])

  plt.xlabel("Time (Seconds)")
  ax.legend()
  plot_name = subfolder + "/simulation.png"
  print("Plot", plot_name)
  fig.savefig(plot_name)
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
  for i in range(len(results)):
    total_ranges.append([results[i][0] - start_time, 1])
    total_ranges.append([results[i][2] - start_time, -1])
    pending_ranges.append([results[i][0] - start_time, 1])
    pending_ranges.append([results[i][1] - start_time, -1])
    active_ranges.append([results[i][1] - start_time, 1])
    active_ranges.append([results[i][2] - start_time, -1])

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


def process_nodes(subfolder):
  node_times = []
  folder = subfolder + "/nodes/"
  regex = [NODE_START_REGEX, NODE_END_REGEX]
  files = os.listdir(folder)
  for file in files:
    node_times.append(process_data(folder + file, regex))

  start_time = min(list(map(lambda r: r[0], node_times)))
  ranges = []
  for i in range(len(node_times)):
    ranges.append([node_times[i][0] - start_time, 1])
    ranges.append([node_times[i][1] - start_time, -1])

  ranges = sorted(ranges, key=lambda r: r[0])
  results = []
  num_nodes = 0
  for i in range(len(ranges)):
    results.append([ranges[i][0] - 1, num_nodes])
    num_nodes += ranges[i][1]
    results.append([ranges[i][0], num_nodes])

  return [start_time, results]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--subfolder", type=str, required=True, help="Folder containing data to graph")
  args = parser.parse_args()
  [start_time, num_nodes] = process_nodes(args.subfolder)
  [active_tasks, pending_tasks, total_tasks] = process_tasks(start_time, args.subfolder)

  numbers = [num_nodes, active_tasks, pending_tasks, total_tasks]
  colors = ["red", "blue", "gray", "purple"]
  labels = ["Number of Servers", "Number of Running Jobs", "Number of Pending Jobs", "Number of Total Jobs"]
  graph(args.subfolder, numbers, colors, labels)


if __name__ == "__main__":
  main()

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


def graph(subfolder, numbers, colors, labels, start_range=None, end_range=None):
  fig, ax = plt.subplots()
  min_timestamp = None
  max_timestamp = None
  for i in range(len(numbers)):
    num = numbers[i]
    timestamps = list(map(lambda r: r[0], num))
    min_t = min(timestamps)
    max_t = max(timestamps)
    if min_timestamp:
      min_timestamp = min(min_timestamp, min_t)
      max_timestamp = max(max_timestamp, max_t)
    else:
      min_timestamp = min_t
      max_timestamp = max_t
    total = list(map(lambda r: r[1], num))
    plt.plot(timestamps, total, color=colors[i], label=labels[i])

  plt.xlabel("Time (Seconds)")
  if start_range:
    min_timestamp = start_range
  if end_range:
    max_timestamp = end_range

  plt.xlim([min_timestamp, max_timestamp])
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
    print(results[i][0] - start_time, results[i][2] - start_time)
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
    ranges.append([st, 1])
    ranges.append([et, -1])
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
  parser.add_argument("--subfolder", type=str, required=True, help="Folder containing data to graph")
  parser.add_argument("--start_range", type=int, help="Start timestamp of zoom region")
  parser.add_argument("--end_range", type=int, help="End timestamp of zoom region")
  args = parser.parse_args()
  [start_time, num_nodes] = process_nodes(args.subfolder)
  [active_tasks, pending_tasks, total_tasks] = process_tasks(start_time, args.subfolder)

  numbers = [num_nodes, active_tasks, pending_tasks, total_tasks]
  colors = ["red", "blue", "gray", "purple"]
  labels = ["Number of Servers", "Number of Running Jobs", "Number of Pending Jobs", "Number of Total Jobs"]
  graph(args.subfolder, numbers, colors, labels, args.start_range, args.end_range)


if __name__ == "__main__":
  main()

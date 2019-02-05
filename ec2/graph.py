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


def graph(node_results, node_color, max_y_height, task_results, task_colors, task_labels):
  fig, ax = plt.subplots()
  y_values = list(map(lambda r: r % max_y_height, range(len(task_results))))
  offsets = list(map(lambda r: 0.0, task_results))
  handles = []
  for i in range(len(task_results[0]) - 1):
    for j in range(len(offsets)):
      offsets[j] += task_results[j][i]
    x_values = list(map(lambda r: r[i + 1], task_results))
    handles.append(ax.barh(y_values, x_values, left=offsets, color=task_colors[i]))

  timestamps = list(map(lambda r: r[0], node_results))
  num_nodes = list(map(lambda r: r[1], node_results))
  plt.plot(timestamps, num_nodes, color=node_color)

  plt.xlabel("Time (Seconds)")
  plt.legend(handles, task_labels)
  plot_name = "simulation.png"
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
  folder = "simulations/" + subfolder + "/tasks/"
  regex = [S3_REGEX, TASK_START_REGEX, TASK_END_REGEX]
  files = os.listdir(folder)
  for file in files:
    results.append(process_data(folder + file, regex))

  results = list(map(lambda r: format_data(r, start_time), results))
  results = sorted(results, key=lambda r: r[0])
  return results


def process_nodes(subfolder):
  node_times = []
  folder = "simulations/" + subfolder + "/nodes/"
  regex = [NODE_START_REGEX, NODE_END_REGEX]
  files = os.listdir(folder)
  print(folder)
  print(files)
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
  subfolder = "uniform-default"
  [start_time, node_results] = process_nodes(subfolder)
  task_results = process_tasks(start_time, subfolder)
  colors = ["gray", "blue"]
  labels = ["Idle", "Tide"]
  node_color = "red"
  max_y_height = 100
  graph(node_results, node_color, max_y_height, task_results, colors, labels)


if __name__ == "__main__":
  main()

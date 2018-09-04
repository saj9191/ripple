import json
import matplotlib
import numpy as np
from matplotlib.font_manager import FontProperties
matplotlib.use('Agg')
import matplotlib.pyplot as plt


COLORS = ["red", "cyan", "yellow", "purple", "orange", "green", "blue"]


def graph(key, dependencies, heights, num_offsets, runtimes, lefts, num_threads, parent_id, layer=0, thread_id=0):
  if thread_id >= num_threads:
    print("Sad", "layer", layer, "thread_id", thread_id)
    print("duration", dependencies[key]["duration"])
    return thread_id

  runtimes[layer][thread_id] += float(dependencies[key]["duration"]) / 1000
  lefts[layer][thread_id] += float(dependencies[key]["timestamp"])
  num_offsets[layer][thread_id] += 1

  children = dependencies[key]["children"]
  children = sorted(children, key=lambda c: -1 * dependencies[c]["length"])

  parent_id = thread_id
  for i in range(len(children)):
    child = children[i]
    thread_id = parent_id + i * max(heights[layer + 1], 0)
    graph(child, dependencies, heights, num_offsets, runtimes, lefts, num_threads, parent_id, layer + 1, thread_id)

  return thread_id


def get_length(key, dependencies):
  if len(dependencies[key]["children"]) == 0:
    dependencies[key]["length"] = dependencies[key]["timestamp"] + dependencies[key]["duration"]
    return dependencies[key]["duration"]

  length = max(list(map(lambda c: get_length(c, dependencies), dependencies[key]["children"])))
  dependencies[key]["length"] = length
  return length


def get_heights(key, dependencies, heights, layer=0):
  if len(dependencies[key]["children"]) == 0:
    h = 1
  else:
    h = 0
    for child in dependencies[key]["children"]:
      h += get_heights(child, dependencies, heights, layer + 1)

  if layer not in heights:
    heights[layer] = 0

  if layer > 10:
    if layer in [11, 12, 18]:
      h = 5
    elif layer not in [13]:
      h = 1
    heights[layer] = h
  elif layer in [3, 4, 9]:
      h = 5
      heights[layer] = h
  else:
    heights[layer] = max(heights[layer], h)
  if layer == 0:
    print("layer", layer, heights[layer])
  return h


def get_plot_data(results, num_layers, params):
  layer_to_count = {}
  dep_file = "results/{0:s}/{1:f}-{2:d}/deps".format(params["folder"], results[0].params["now"], results[0].params["nonce"])
 # dep_file_format = "results/concurrency{0:d}/{1:f}/{2:f}-{3:d}/deps"
  #dep_file = dep_file_format.format(len(results), params["timestamp"], results[0].params["now"], results[0].params["nonce"])
  dependencies = json.loads(open(dep_file).read())
  for key in dependencies.keys():
    if ":" not in key:
      continue
    layer = int(key.split(":")[0])
    if layer == 2:
      print(layer, key, dependencies[key]["parent_key"])
    if layer not in layer_to_count:
      layer_to_count[layer] = 0
    layer_to_count[layer] += 1

  num_threads = int(max(layer_to_count.values()) * 1)
  lefts = list(map(lambda l: [0] * num_threads, range(num_layers)))
  runtimes = list(map(lambda l: [0] * num_threads, range(num_layers)))
  num_offsets = list(map(lambda l: [0] * num_threads, range(num_layers)))

  heights = {}
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    dependencies = json.loads(open("results/{0:s}/{1:s}/deps".format(params["folder"], dep_folder)).read())
    #dependencies = json.loads(open("results/concurrency{0:d}/{1:f}/{2:s}/deps".format(len(results), params["timestamp"], dep_folder)).read())
    root_key = list(filter(lambda k: k.startswith("0:"), dependencies.keys()))[0]
    get_length(root_key, dependencies)
    if len(heights) == 0:
      get_heights(root_key, dependencies, heights)
      heights[0] = num_threads
      heights[1] = int(num_threads / 92)
      heights[1] = int(num_threads / 92)
    graph(root_key, dependencies, heights, num_offsets, runtimes, lefts, num_threads, -1, layer=0, thread_id=0)

  for layer in range(len(runtimes)):
    for thread_id in range(len(runtimes[layer])):
      num = max(num_offsets[layer][thread_id], 1)
      runtimes[layer][thread_id] = float(runtimes[layer][thread_id]) / num
      lefts[layer][thread_id] = float(lefts[layer][thread_id]) / num

  return [dependencies, lefts, runtimes, heights, num_threads]


def runtime_plot(num_layers, num_results, lefts, runtimes, heights, num_threads, pipeline, params):
  threads = range(1, num_threads + 1)
  print("Num threads", num_threads)

  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  labels = []
  legends = []
  dark_colors = ["purple", "brown", "blue", "green"]
  for i in range(num_layers):
    left = lefts[i]
    runtime = runtimes[i]
    if i in [14, 15, 16, 17]:
      edgecolors = list(map(lambda r: "none", runtime))
      color = dark_colors[i % len(dark_colors)]
    else:
      edgecolors = list(map(lambda r: "black" if r > 0 else "none", runtime))
      color = COLORS[i % len(COLORS)]

    if len(heights) > i:
      height = heights[i] if heights[i] == 1 else heights[i]
    else:
      height = 0
    alpha = 0.4
    p = ax.barh(threads, runtime, color=color, left=left, height=height, align="edge", edgecolor=edgecolors, linewidth=1, alpha=alpha)

    legends.append(p[0])
    labels.append(pipeline[i]["name"] + " ({0:d})".format(i))

  fontP = FontProperties()
  fontP.set_size('x-small')
  fig.tight_layout(rect=[0, 0, 0.85, 0.94])
  fig.legend(legends, labels, prop=fontP, loc="upper left", ncol=1, bbox_to_anchor=(0.75, 0.95))
  plt.yticks([])
  ax.set_ylim(0, num_threads)
  ax.set_xlim(0, 500)
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime Concurrency {0:d} ({1:f})".format(num_results, params["timestamp"]))
  plot_name = "results/concurrency{0:d}/{1:f}/plot-{0:d}.png".format(num_results, params["timestamp"])
  print("plot", plot_name)
  fig.savefig(plot_name)
  plt.close()


def error_plot(num_results, num_layers, results, pipeline, params):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  bottom = 0
  legends = []
  labels = []

  runtimes = list(map(lambda l: [], range(num_layers)))
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    dependencies = json.loads(open("results/concurrency{0:d}/{1:f}/{2:s}/deps".format(len(results), params["timestamp"], dep_folder)).read())
    for key in dependencies.keys():
      layer = int(key.split(":")[0])
      runtimes[layer].append(dependencies[key]["duration"] / 1000)

  for layer in range(num_layers):
    runtime = runtimes[layer]
    mean = np.mean(runtime)
    std = np.std(runtime)
    p = ax.bar([0], [mean], bottom=[bottom])
    color = p.patches[0].get_facecolor()
    offset = 0.3
    color = (min(color[0] + offset, 1.0), min(color[1] + offset, 1.0), min(color[2] + offset, 1.0))

    x_pos = (layer % 5) * 0.15 - 0.25
    ax.errorbar(x_pos, bottom + mean, yerr=std, ecolor=color, capsize=4)
    ax.errorbar(x_pos, bottom, yerr=std, ecolor=color, capsize=4)
    legends.append(p[0])
    labels.append(pipeline[layer]["name"])
    bottom += mean

  fig.tight_layout(rect=[0, 0.05, 0.65, 0.90])
  plt.xticks([])
  fig.legend(legends, labels, loc="upper right")
  plt.title("Error Concurrency {0:d} ({1:f})".format(num_results, params["timestamp"]))
  plot_name = "results/concurrency{0:d}/{1:f}/error-{0:d}.png".format(num_results, params["timestamp"])
  fig.savefig(plot_name)
  print("Error plot", plot_name)
  plt.close()


def ec2_accumulation_plot(results, params):
  points = []
  regions = {}

  labels = []
  print(len(results))
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    stats = json.loads(open("{0:s}/{1:s}/stats".format(params["folder"], dep_folder)).read())["stats"]

    layer = 0
    start = 0
    for stat in stats:
      stat = stat[0]
      if "name" not in stat:
        stat["name"] = "terminate"
      if stat["name"] in ["load", "total"]:
        continue

      labels.append(stat["name"])
      if layer not in regions:
        regions[layer] = [0, 0]

      regions[layer][0] += start
      if stat["name"] in ["download", "ssw", "upload"]:
        duration = stat["billed_duration"][0]
      else:
        duration = stat["billed_duration"]
      duration /= 1000.0
      start += duration
      regions[layer][1] += start
      layer += 1

  for layer in regions.keys():
    for i in range(2):
      regions[layer][i] = regions[layer][i] / len(results)

  x = [0, regions[len(regions) - 1][1]]
  y = [1, 1]

  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)

  colors = ["red", "blue", "yellow", "purple", "orange", "green", "magenta"]
  legends = []
  alpha = 0.3
  for layer in range(len(regions)):
    color = colors[layer % len(colors)]
    ax.axvspan(regions[layer][0], regions[layer][1], facecolor=color, alpha=alpha)
    legends.append(matplotlib.lines.Line2D([0], [0], color=color, alpha=alpha, label=labels[layer]))

  fontP = FontProperties()
  fontP.set_size('x-small')
  fig.tight_layout(rect=[0.05, 0.05, 0.80, 0.90])
  fig.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.0, 0.95))
  plt.xlim(x)
  plt.title("Average Accumulation (EC2)".format(len(results)))
  ax.plot(x, y, color="black")
  plot_name = "{0:s}/accumulation.png".format(params["folder"])
  plt.xlabel("Runtime (seconds)")
  plt.ylabel("Number of EC2 instances")
  print(plot_name)
  fig.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()


def accumulation_plot(x, y, regions, pipeline, title, plot_name, folder):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)

  legends = []
  alpha = 0.3
  for layer in regions.keys():
    color = COLORS[layer % len(COLORS)]
    ax.axvspan(regions[layer][0], regions[layer][1], facecolor=color, alpha=alpha)
    legends.append(matplotlib.lines.Line2D([0], [0], color=color, alpha=alpha, label=pipeline[layer]["name"]))

  fontP = FontProperties()
  fontP.set_size('x-small')
  fig.tight_layout(rect=[0.05, 0.05, 0.80, 0.90])
  fig.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.0, 0.95))
  plt.xlim([0, x[-1]])
  plt.title(title)
  ax.plot(x, y, color="black")
  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)")
  plt.ylabel("Number of Lambda Processes")
  print(plot_name)
  fig.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()


def plot(results, pipeline, params):
  num_layers = len(pipeline)
  num_results = len(results)
  #[dependencies, lefts, runtimes, heights, num_threads] = get_plot_data(results, num_layers, params)
  #runtime_plot(num_layers, num_results, lefts, runtimes, heights, num_threads, pipeline, params)
  #error_plot(num_results, num_layers, results, pipeline, params)
#  if params["model"] == "ec2":
#    ec2_accumulation_plot(results, params)
#  else:
  accumulation_plot(num_results, num_layers, results, pipeline, params)


def comparison(name, title, lambda_result, ec2_result, ylabel, params={}):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ind = range(2)

  ax.bar([0], [lambda_result], color="red", bottom=0)
  ax.bar([1], [ec2_result], color="blue", bottom=0)

  print("Comparison", lambda_result, ec2_result)
  plt.xticks(ind, ("Lambda", "EC2"))
  plt.title(title)
  plt.ylabel(ylabel)
  file_name = "{0:s}.png".format(name)
  fig.savefig(file_name)
  print("Comparison", file_name)
  plt.close()


if __name__ == "__main__":
  params = json.loads(open("json/smith-waterman.json").read())
  pipeline = params["pipeline"]
  filename = "dep-1533567226.383849-70"
  dependencies = json.loads(open("results/{0:s}".format(filename)).read())
  parts = filename.split("-")
  params["now"] = float(parts[1])
  params["nonce"] = int(parts[2])
  plot(dependencies, pipeline, 1, params)

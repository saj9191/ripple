import json
import matplotlib
import numpy as np
from matplotlib.font_manager import FontProperties
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


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


def accumulation_plot(x, y, regions, pipeline, title, plot_name, folder):
  if plot_name.startswith("ssw"):
    offset = 0
    colors = ["purple", "cyan", "blue", "red", "orange", "green", "black"]
  elif plot_name.startswith("methyl"):
    colors = ["purple", "cyan", "black"]
    offset = 3
  else:
    colors = ["purple", "cyan", "blue", "red", "orange", "green", "brown", "black"]
    offset = 1

  fig = plt.figure()
  rows = 12
  ax1 = plt.subplot2grid((rows, 1), (0, 0), rowspan=rows - 1)

  legends = []
  alpha = 1
  for layer in regions.keys():
    color = colors[layer % len(colors)]
    patch = mpatches.Patch(facecolor=color, edgecolor="black", label=pipeline[layer]["name"], linewidth=1, linestyle="solid")
    legends.append(patch)
  fontP = FontProperties(family="Arial", size="small")
  fig.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.02, 1.02), framealpha=0, borderpad=1)

  max_x = regions[len(regions) - 1][1]
  plt.xlim([0, max_x])
  plt.xticks([])

  for layer in x:
    px = []
    py = []
    color = colors[layer % len(colors)]
    x0 = regions[layer][0]
    x1 = regions[layer][1]
    max_y = 0
    for i in range(len(x[layer])):
      if (x0 <= x[layer][i] and x[layer][i] <= x1):
        px.append(min(x[layer][i], max_x - offset))
        py.append(y[layer][i])
      elif x[layer][i] > x1:
        max_y = max(y[layer][i], max_y)
    px.append(min(x1, max_x - offset))
    py.append(max_y)
    ax1.plot(px, py, color=color)

  for side in ["right", "top"]:
    ax1.spines[side].set_visible(False)

  for side in ["left", "bottom"]:
    ax1.spines[side].set_linewidth(3)

  font_size = 16
  plt.ylabel("Number of Lambda Processes", size=font_size)

  ax2 = plt.subplot2grid((rows, 1), (rows - 1, 0))
  line_width = 4.0
  for layer in regions.keys():
    y = 0.4 * (layer % 3)
    color = colors[layer % len(colors)]
    x0 = max(regions[layer][0], offset)
    x1 = min(regions[layer][1], max_x - offset)
    ax2.plot([x0, x1], [y, y], color="black", linewidth=line_width + 2)
    ax2.plot([x0, x1], [y, y], color="white", linewidth=line_width)
    ax2.plot([x0, x1], [y, y], color=color, linewidth=line_width, alpha=alpha)

  for side in ["bottom", "left", "right", "top"]:
    ax2.spines[side].set_visible(False)

  plt.xlim([0, max_x])
  plt.ylim([-0.5, 1.0])
  plt.yticks([])
  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)", size=font_size)
  print(plot_name)
  fig.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()
  return


def plot(results, pipeline, params):
  num_layers = len(pipeline)
  num_results = len(results)
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


def duration_statistics(folder, lambda_stats):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  y = lambda_stats.values()
  ax.boxplot(y, whis="range")

  plot_name = "{0:s}/box_plot.png".format(folder)
  plt.ylabel("Runtime (seconds)")
  print(plot_name)
  fig.savefig(plot_name)
  print("Box plot", plot_name)
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

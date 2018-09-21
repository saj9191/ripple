import boto3
import json
import matplotlib
import numpy as np
import sys
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


def accumulation_plot(x, y, pipeline, title, plot_name, folder, absolute=False, zoom=None):
  print(plot_name)
  if plot_name.startswith("ssw"):
    offset = 0
    colors = ["purple", "cyan", "orange", "blue", "magenta", "black"]
    ys = [0.15, 0.40, 0.65, 0.90, 1.15]
    num_rows = 4
    increment = 1
  elif plot_name.startswith("methyl"):
    colors = ["purple", "cyan", "black"]
    offset = 3
    ys = [0.15, 0.30, 0.45, 0.60, 0.75, 1]
    num_rows = 6
    increment = 5
  elif plot_name.startswith("knn"):
    colors = ["red", "purple", "orange", "blue", "green", "black"]
    ys = [0.15, 0.40, 0.65, 0.90, 1.15]
    num_rows = 4
    increment = 5
  else:
    colors = ["purple", "cyan", "blue", "red", "orange", "green", "brown", "magenta", "black"]
    offset = 1
    ys = [0.15, 0.30, 0.45, 0.60, 0.75, 1]
    num_rows = 5
    increment = 5

  rows = 6
  font_size = 16

  fig1 = plt.figure()
  offset = 1
  if absolute:
    ax1 = plt.subplot(2, 1, 1)
    plt.ylabel("Number of Lambda Processes", size=font_size)
    ax1.yaxis.set_label_coords(-0.08, -0.05)
    ax2 = plt.subplot(2, 1, 2)
  else:
    ax1 = plt.subplot2grid((rows, 1), (0, 0), rowspan=rows - offset)
    ax2 = plt.subplot2grid((rows, 1), (rows - offset, 0), rowspan=offset)

  legends = []

  if not absolute:
    for layer in range(len(pipeline) + 1):
      color = colors[layer % len(colors)]
      label = pipeline[layer]["name"] if layer < len(pipeline) else "Total"
      patch = mpatches.Patch(facecolor=color, edgecolor="black", label=label, linewidth=1, linestyle="solid")
      legends.append(patch)
    fontP = FontProperties(family="Arial", size="small")
    fig1.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.02, 1.02), framealpha=0, borderpad=1)

  regions = {}
  for layer in x:
    min_x = min_y = sys.maxsize
    max_x = max_y = 0
    px = []
    py = []
    color = colors[layer % len(colors)]
    for i in range(len(x[layer])):
      lx = x[layer][i]
      ly = y[layer][i]
      if len(py) > 0 and lx - px[-1] > increment:
        r = range(int(px[-1]) + increment, int(lx), increment)
        for dx in r:
          px.append(dx)
          py.append(py[-1])
      if ly > 0:
        px.append(lx)
        py.append(ly)

    if len(py) > 0:
      min_y = min(min_y, min(py))
      max_y = max(max_y, max(py))
      min_x = min(min_x, min(px))
      max_x = max(max_x, max(px))

    regions[layer] = [min_x, max_x]
    s = 1 if color != "black" else 0.05
    alpha = 1 if color != "black" else 1
    ax1.scatter(px, py, color=color, s=s, alpha=alpha)
    if absolute:
      ax2.scatter(px, py, color=color, s=s, alpha=alpha)

  for side in ["right", "top"]:
    ax1.spines[side].set_visible(False)
    if absolute:
      ax2.spines[side].set_visible(False)

  for side in ["left", "bottom"]:
    ax1.spines[side].set_linewidth(3)
    if absolute:
      ax2.spines[side].set_linewidth(3)

  if not absolute:
    line_width = 6.0
    for layer in range(len(regions) - 1):
      y = ys[layer % num_rows]
      color = colors[layer % len(colors)]
      x0 = max(regions[layer][0], offset)
      x1 = min(regions[layer][1], max_x - offset)
      ax2.plot([x0, x1], [y, y], color="black", linewidth=line_width + 2)
      ax2.plot([x0, x1], [y, y], color="white", linewidth=line_width)
      ax2.plot([x0, x1], [y, y], color=color, linewidth=line_width)

    for side in ["bottom", "left", "right", "top"]:
      ax2.spines[side].set_visible(False)

  if absolute:
    ax1.set_xlim(zoom)
    ax2.set_xlim([0, max_x])
    ax1.set_ylim([0, max_y])
  else:
    ax1.set_xticks([])
    ax1.set_xlim([0, max_x])
    ax2.set_xlim([0, max_x])
    ax2.set_ylim([0, 1.2])
    ax2.set_yticks([])
    ax1.set_ylabel("Number of Running Processes")

  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)", size=font_size)
  print(plot_name)
  fig1.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()
  return


def plot(results, pipeline, params):
  num_layers = len(pipeline)
  num_results = len(results)
  accumulation_plot(num_results, num_layers, results, pipeline, params)


def comparison(name, title, lambda_costs, ec2_cost, ec2_s3_cost, ylabel, params={}):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ind = range(2)

  ax.bar([2], [ec2_cost], color="orange", bottom=0)
  ax.bar([3], [ec2_s3_cost], color="purple", bottom=0)

  #print("Comparison", lambda_result, ec2_result)
  plt.xticks(ind, ("Lambda Cost", "Lambda S3 Cost", "EC2", "EC2 S3 Cost"))
  plt.title(title)
  plt.ylabel(ylabel)
  file_name = "{0:s}.png".format(name)
  fig.savefig(file_name)
  print("Comparison", file_name)
  plt.close()


def statistics(name, folder, stats, labels, ty):
  fig, ax = plt.subplots()
  s3 = boto3.resource("s3")
  keys = set(stats[0].keys())
  for s in stats[1:]:
    keys = keys.intersection(set(s.keys()))

  p = []
  for key in keys:
    if name != "Smith Waterman":
      if key.startswith("TN_"):
        bucket = "shjoyner-als"
      elif key.startswith("22Feb"):
        bucket = "shjoyner-sample-input"
      else:
        bucket = "shjoyner-ash"
    else:
      bucket = "ssw-input"
    print(bucket, key)
    p.append([s3.Object(bucket, key).content_length, key])

  p.sort()
  keys = list(map(lambda k: k[1], p))

  colors = ["red", "blue", "green", "purple"][:len(stats)]
  for i in range(len(keys)):
    key = keys[i]
    runtimes = []
    for j in range(len(stats)):
      runtimes.append(stats[j][key])
    start = i * (len(stats) + 1)
    positions = range(start + 1, start + len(stats) + 1)
    bp = ax.boxplot(runtimes, positions=positions, widths=0.7, whis="range")
    for j in range(len(bp["boxes"])):
      c = colors[j]
      plt.setp(bp["boxes"][j], color=c)
      plt.setp(bp["medians"][j], color=c)
      plt.setp(bp["fliers"][j], color=c)
      for k in range(2):
        plt.setp(bp["caps"][2 * j + k], color=c)
        plt.setp(bp["whiskers"][2 * j + k], color=c)

  legend = []
  for i in range(len(labels)):
    legend.append(plt.Line2D([0], [0], color=colors[i], lw=1, label=labels[i]))
  ax.legend(handles=legend, loc="upper right", bbox_to_anchor=(1.1, 1.05))
  plt.xlim([0, len(keys) * (len(stats) + 1) + 1])
  plot_name = "{0:s}/{1:s}_box_plot.png".format(folder, ty)

  ticks = []#list(map(lambda q: "%.1fMB" % (q[0] / 1024 / 1024), p))
  for i in range(len(p)):
    ticks.append("")
    ticks.append("")
    x = p[i][0]
    if x < 1024:
      ticks.append("%.1fB" % x)
    elif x < (1024 * 1024):
      x /= 1024
      ticks.append("%.1fKB" % x)
    else:
      x /= (1024 * 1024)
      ticks.append("%.1fMB" % x)
  print(ticks)
  ax.set_xticks(range(len(ticks)))
  ax.set_xticklabels(ticks)

  if ty == "duration":
    plt.ylabel("Runtime (Seconds)")
  else:
    plt.ylabel("Cost ($)")
  plt.title(name)
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

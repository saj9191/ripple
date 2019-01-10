import argparse
import boto3
import json
import matplotlib
import numpy as np
import os
import sys
from matplotlib.font_manager import FontProperties
from matplotlib import mlab
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec


def plot_spectra_comparison(x_labels, y_values, section_labels, folder):
  ind = range(len(y_values))
  legend_objs = []
  bottoms = list(map(lambda b: 0, section_labels))
  for label in section_labels:
    values = list(map(lambda v: v[label], y_values))
    legend_objs.append(plt.bar(ind, values, bottom=bottoms))
    for i in range(len(values)):
      bottoms[i] += values[i]

  plt.ylabel("Duration (Seconds)")
  plt.title("Number of Spectra Runtime Comparison")
  plt.xticks(ind, x_labels)
  plt.legend(legend_objs, section_labels)
  plt.xlabel("Runtime (seconds)")
  plot_name = folder + "/comparison.png"
  plt.savefig(plot_name)
  print("plot_spectra_comparison", plot_name)
  plt.close()


def process_num_spectra(file):
  stats = json.loads(open(file).read())["stats"]
  stages = {-1: [sys.maxsize, 0]}
  ranges = {"species": [0, 5], "sort": [6, 12], "tide": [13, 15], "percolator": [16, 16]}

  messages = {}
  for i in range(len(stats)):
    stage_stats = stats[i]
    for r in ranges:
      if ranges[r][0] <= i and i <= ranges[r][1]:
        stage = r

    if stage not in stages:
      stages[stage] = [sys.maxsize, 0]
      messages[stage] = 0

    messages[stage] += len(stage_stats["messages"])
    for message in stage_stats["messages"]:
      start_time = message["start_time"]
      end_time = start_time + message["duration"] / 1000.0
      for s in [stage, -1]:
        stages[s][0] = min(start_time, stages[s][0])
        stages[s][1] = max(end_time, stages[s][1])

  counts = {
    5000: {"tide": 150, "species": 48, "sort": 268, "percolator": 1},
   10000: {"tide": 150, "species": 48, "sort": 533, "percolator": 1},
   15000: {"tide": 150, "species": 138, "sort": 851, "percolator": 1},
   20000: {"tide": 150, "species": 48, "sort": 1275, "percolator": 1},
  }

  for stage in stages:
    if stage != -1:
      num_spectra = int(file.split("-")[-1])
      print(file, stage, messages[stage])
      assert(counts[num_spectra][stage] == messages[stage])
      stages[stage][0] -= stages[-1][0]
      stages[stage][1] -= stages[-1][0]

  return stages


def num_spectra_comparison():
  def average(results):
    avg = {}
    for num_spectra in results:
      print("num_spectra", num_spectra, "num", len(results[num_spectra]))
      avg[num_spectra] = {}
      for result in results[num_spectra]:
        for stage in result:
          if stage not in avg[num_spectra]:
            avg[num_spectra][stage] = 0
          duration = result[stage][1] - result[stage][0]
          #print("num_spectra", num_spectra, "stage", stage, "duration", duration)
          avg[num_spectra][stage] += duration
#        print("")

      for stage in avg[num_spectra]:
        avg[num_spectra][stage] = float(avg[num_spectra][stage]) / len(results[num_spectra])
    return avg

  folder = "nature_results/num_spectra"
  results = {}
  for root, dirs, files in os.walk(folder):
    if root == folder:
      for file in files:
        num_spectra = int(file.split("-")[-1])
        if num_spectra not in results:
          results[num_spectra] = []
        results[num_spectra].append(process_num_spectra(folder + "/" + file))

  avg_results = average(results)
  print(avg_results)
  x_labels = sorted(avg_results.keys())
  y_values = list(map(lambda label: avg_results[label], x_labels))
  plot_spectra_comparison(x_labels, y_values, ["species", "sort", "tide", "percolator"], folder + "/figures")


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


def accumulation_plot(x, y, points, pipeline, plot_name, folder, absolute=None):
  if plot_name.startswith("ssw"):
    offset = 0
    colors = ["purple", "cyan", "orange", "blue", "magenta", "black"]
    ys = [0.15, 0.40, 0.66, 0.92, 1.15]
    num_rows = 4
    rows = 7
    increment = 1
  elif plot_name.startswith("methyl"):
    colors = ["purple", "cyan", "black"]
    offset = 3
    ys = [0.15, 0.30, 0.45, 0.60, 0.75, 1]
    num_rows = 6
    rows = 6
    increment = 5
  elif plot_name.startswith("knn"):
    colors = ["red", "purple", "orange", "blue", "green", "black"]
    ys = [0.25, 0.45, 0.65, 0.85]
    num_rows = 4
    rows = 6
    increment = 5
  elif plot_name.startswith("compression"):
    colors = ["purple", "cyan", "black"]
    offset = 1
    ys = [0.14, 0.35, 0.55, 0.76, 0.96, 1.15]
    num_rows = 5
    rows = 5
    increment = 1
  elif plot_name.startswith("decompression"):
    colors = ["cyan", "black"]
    offset = 1
    ys = [0.14]
    num_rows = 1
    rows = 6
    increment = 1
  else:
    colors = ["purple", "cyan", "blue", "red", "orange", "green", "brown", "magenta", "black"]
    offset = 1
    ys = [0.34, 0.53, 0.72, 0.91, 1.10, 1.14]
    num_rows = 5
    rows = 5
    increment = 5

  plt.subplots_adjust(hspace=0, wspace=0)
  font_size = 16

  fig1 = plt.figure()
  offset = 1
  if absolute:
    gs1 = gridspec.GridSpec(2, 1)
    ax1 = plt.subplot(gs1[0])
    #plt.ylabel("Number of Lambda Processes", size=font_size)
    ax2 = plt.subplot(gs1[1])
#    ax1.yaxis.set_label_coords(-0.08, 1)
  else:
    ax1 = plt.subplot2grid((rows, 1), (0, 0), rowspan=rows - offset)
    ax2 = plt.subplot2grid((rows, 1), (rows - offset, 0), rowspan=offset)

  legends = []
  ax1.margins(x=0, y=0)
  ax2.margins(x=0, y=0)
  fontP = FontProperties(family="Arial", size="medium")

  for layer in range(len(pipeline) + 1):
    color = colors[layer % len(colors)]
    label = pipeline[layer]["name"] if layer < len(pipeline) else "Total"
    patch = mpatches.Patch(facecolor=color, edgecolor="black", label=label, linewidth=1, linestyle="solid")
    legends.append(patch)
  if not absolute:
    if plot_name.startswith("ssw"):
      fig1.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.02, 1.02), framealpha=0, borderpad=1)
    elif plot_name.startswith("knn"):
      fig1.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(0.90, 0.94), framealpha=0, borderpad=1)
    else:
      fig1.legend(handles=legends, loc="upper right", prop=fontP, bbox_to_anchor=(1.00, 1.02), framealpha=0, borderpad=1)

  regions = {}
  max_y = 0
  max_x = 0
  for layer in x:
    min_x = sys.maxsize
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
      max_y = max(max_y, max(py))
      min_x = min(min_x, min(px))
      max_x = max(max_x, max(px))

    regions[layer] = [min_x, max_x]
    ax1.plot(px, py, color=color)
    if absolute is not None:
      ax2.plot(px, py, color=color)

  for side in ["right", "top"]:
    ax1.spines[side].set_visible(False)
    if absolute is not None:
      ax2.spines[side].set_visible(False)

  for side in ["left", "bottom"]:
    ax1.spines[side].set_linewidth(3)
    if absolute is not None:
      ax2.spines[side].set_linewidth(3)

  if not absolute:
    line_width = 6
    for layer in range(len(regions) - 1):
      y = ys[layer % num_rows]
      color = colors[layer % len(colors)]
      x0 = regions[layer][0]
      x1 = regions[layer][1]
      max_x = max(max_x, x1)
      ax2.plot([x0, x1], [y, y], color="black", linewidth=line_width + 2)
      ax2.plot([x0, x1], [y, y], color=color, linewidth=line_width)

    for side in ["bottom", "left", "right", "top"]:
      ax2.spines[side].set_visible(False)

  if absolute is not None:
    ax1.set_xlim(absolute)
    ax2.set_xlim([0, max_x])
  else:
    ax1.set_xticks([])
    ax1.set_xlim([0, max_x])
    ax2.set_xlim([0, max_x])
    ax2.set_ylim([0, max(ys) + 0.1])
    ax2.set_yticks([])
    ax1.set_ylabel("Number of Running Processes", size=font_size)

  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)", size=font_size)
  print(plot_name)
  if not absolute:
    if plot_name.startswith("ssw"):
      fig1.tight_layout(rect=[0, 0, 0.75, 1], h_pad=0)
    elif plot_name.startswith("knn"):
      fig1.tight_layout(rect=[0, 0, 0.90, 1], h_pad=0)
    else:
      fig1.tight_layout(rect=[0, 0, 0.92, 1], h_pad=0)
  else:
    gs1.tight_layout(fig1)
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


def durations(times, folder, plot_name):
  fig, ax = plt.subplots()
  line_width = 1
  font_size = 10
  colors = [
    "#f44336",  # red
    "#ffebee",  # red-50
    "#ffcdd2",  # red-100
    "#ef9a9a",  # red-200
    "#e57373",  # red-300
    "#ef5350",  # red-400
    "#f44336",  # red-500
    "#e53935",  # red-600
    "#d32f2f",  # red-700
    "#c62828",  # red-800
    "#b71c1c",  # red-900
    "#ff8a80",  # red-a100
    "#ff5252",  # red-a200
    "#ff1744",  # red-a400
    "#d50000",  # red-a700
    "#2196f3",  # blue
    "#e3f2fd",  # blue-50
    "#bbdefb",  # blue-100
    "#90caf9",  # blue-200
    "#64b5f6",  # blue-300
    "#42a5f5",  # blue-400
    "#2196f3",  # blue-500
    "#1e88e5",  # blue-600
    "#1976d2",  # blue-700
    "#1565c0",  # blue-800
    "#0d47a1",  # blue-900
    "#82b1ff",  # blue-a100
    "#448aff",  # blue-a200
    "#2979ff",  # blue-a400
    "#2962ff",  # blue-a700
    "#e91e63",  # pink
    "#fce4ec",  # pink-50
    "#f8bbd0",  # pink-100
    "#f48fb1",  # pink-200
    "#f06292",  # pink-300
    "#ec407a",  # pink-400
    "#e91e63",  # pink-500
    "#d81b60",  # pink-600
    "#c2185b",  # pink-700
    "#ad1457",  # pink-800
    "#880e4f",  # pink-900
    "#ff80ab",  # pink-a100
    "#ff4081",  # pink-a200
    "#f50057",  # pink-a400
    "#c51162",  # pink-a700
    "#4caf50",  # green
    "#e8f5e9",  # green-50
    "#c8e6c9",  # green-100
    "#a5d6a7",  # green-200
    "#81c784",  # green-300
    "#66bb6a",  # green-400
    "#4caf50",  # green-500
    "#43a047",  # green-600
    "#388e3c",  # green-700
    "#2e7d32",  # green-800
    "#1b5e20",  # green-900
    "#b9f6ca",  # green-a100
    "#69f0ae",  # green-a200
    "#00e676",  # green-a400
    "#00c853",  # green-a700
    "#ffc107",  # amber
    "#fff8e1",  # amber-50
    "#ffecb3",  # amber-100
    "#ffe082",  # amber-200
    "#ffd54f",  # amber-300
    "#ffca28",  # amber-400
    "#ffc107",  # amber-500
    "#ffb300",  # amber-600
    "#ffa000",  # amber-700
    "#ff8f00",  # amber-800
    "#ff6f00",  # amber-900
    "#ffe57f",  # amber-a100
    "#ffd740",  # amber-a200
    "#ffc400",  # amber-a400
    "#ffab00",  # amber-a700
    "#607d8b",  # blue-grey
    "#eceff1",  # blue-grey-50
    "#cfd8dc",  # blue-grey-100
    "#b0bec5",  # blue-grey-200
    "#90a4ae",  # blue-grey-300
    "#78909c",  # blue-grey-400
    "#607d8b",  # blue-grey-500
    "#546e7a",  # blue-grey-600
    "#455a64",  # blue-grey-700
    "#37474f",  # blue-grey-800
    "#263238",  # blue-grey-900
    "#673ab7",  # deep-purple
    "#ede7f6",  # deep-purple-50
    "#d1c4e9",  # deep-purple-100
    "#b39ddb",  # deep-purple-200
    "#9575cd",  # deep-purple-300
    "#7e57c2",  # deep-purple-400
    "#673ab7",  # deep-purple-500
    "#5e35b1",  # deep-purple-600
    "#512da8",  # deep-purple-700
    "#4527a0",  # deep-purple-800
    "#311b92",  # deep-purple-900
    "#b388ff",  # deep-purple-a100
    "#7c4dff",  # deep-purple-a200
    "#651fff",  # deep-purple-a400
    "#6200ea",  # deep-purple-a700
  ]
  print("num colors", len(colors))

  keys = list(map(lambda t: [times[t][0], t], times.keys()))
  keys.sort()
  bars = []

  durations = []
  start_times = []
  for [start_time, job] in keys:
    y = 20 * len(bars)
    color = colors[job]
    bars.append(ax.plot(times[job], [y, y], linewidth=line_width, color=color))
    durations.append(times[job][1] - times[job][0])
    start_times.append(start_time)

  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)", size=font_size)
  plt.yticks([])
  print("Duration", plot_name)
  fig.savefig(plot_name)
  plt.close()


def cdf_plot(folder, name, results, labels):
  bins = []
  for r in results:
    r.sort()
    n, b, patches = plt.hist(r, normed=1, cumulative=True, histtype="step")
    bins.append(b)
    plt.close()

  fig, ax = plt.subplots()
  for i in range(len(bins)):
    y = mlab.normpdf(bins[i], np.average(results[i]), np.std(results[i])).cumsum()
    y /= y[-1]
    ax.plot(bins[i], y, linewidth=2, label=labels[i])

  for tick in ax.xaxis.get_major_ticks():
    tick.label.set_fontsize(16)

  for tick in ax.yaxis.get_major_ticks():
    tick.label.set_fontsize(16)

  plot_name = "{0:s}/{1:s}_cdf.png".format(folder, name)
  print("CDF Duration", plot_name)
  font_size = 20
  ax.set_ylabel("CDF", fontsize=font_size)
  ax.set_xlabel("Start Time", fontsize=font_size)
  legend = ax.legend(fontsize=font_size, frameon=False)
  legend.get_frame().set_facecolor('none')
  fig.tight_layout()#rect=[0, 0.10, 1, 1.10])
  fig.savefig(plot_name)
  plt.close()


def cdf(folder, times, labels):
  durations = list(map(lambda t: [], times))
  start_times = list(map(lambda t: [], times))

  for i in range(len(times)):
    keys = list(map(lambda t: [times[i][t][0], t], times[i].keys()))
    for [start_time, job] in keys:
      durations[i].append(times[i][job][1] - times[i][job][0])
      start_times[i].append(start_time)

  cdf_plot(folder, "duration", durations, labels)
  cdf_plot(folder, "start", start_times, labels)


def statistics(name, folder, stats, labels, ty):
  fig, ax = plt.subplots()
  s3 = boto3.resource("s3")
  keys = set(stats[0].keys())
  for s in stats[1:]:
    keys = keys.intersection(set(s.keys()))

  p = []
  for key in keys:
    bucket = "ssw-input"
    if name != "Smith Waterman":
      if key.startswith("TN_"):
        bucket = "shjoyner-als"
      elif key.startswith("22Feb"):
        bucket = "shjoyner-sample-input"
      elif key.startswith("methyl"):
        bucket = "maccoss-methyl-data"
      else:
        bucket = "shjoyner-ash"
    else:
      bucket = "ssw-input"
    print(bucket, key)
    p.append([s3.Object(bucket, key).content_length, key])

  p.sort()
  keys = list(map(lambda k: k[1], p))
  print("p", p)

  colors = ["red", "skyblue", "green", "purple"][:len(stats)]
  for i in range(len(keys)):
    key = keys[i]
    runtimes = []
    for j in range(len(stats)):
      #runtimes.append(np.average(stats[j][key]))
      runtimes.append(stats[j][key])
    start = i * (len(stats) + 1)
    positions = range(start + 1, start + len(stats) + 1)
    bp = ax.boxplot(runtimes, positions=positions, widths=0.7, whis="range", patch_artist=True)
    for j in range(len(bp["boxes"])):
      c = colors[j]
      bp["boxes"][j].set(facecolor=c)
      plt.setp(bp["boxes"][j], color=c)
      plt.setp(bp["medians"][j], color="black")
      plt.setp(bp["fliers"][j], color="black")
      for k in range(2):
        plt.setp(bp["caps"][2 * j + k], color="black")
        plt.setp(bp["whiskers"][2 * j + k], color="black")

  legend = []
  for i in range(len(labels)):
    legend.append(plt.Line2D([0], [0], color=colors[i], lw=1, label=labels[i]))
  font_size = 16
  ax.legend(handles=legend, loc="upper left", fontsize=font_size)#, bbox_to_anchor=(1.1, 1.05))
  plt.xlim([0, len(keys) * (len(stats) + 1) + 1])
  plot_name = "{0:s}/{1:s}_box_plot.png".format(folder, ty)

  ax.tick_params(axis="x", which="both", length=0)
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
    elif x < (1024 * 1024 * 1024):
      x /= (1024 * 1024)
      ticks.append("%.1fMB" % x)
    else:
      x /= (1024 * 1024 * 1024)
      ticks.append("%.1fGB" % x)
  ax.set_xticks(range(len(ticks)))
  ax.set_yscale("log")
  ax.set_xticklabels(ticks)

  plt.xlabel("Query File Size", size=font_size)
  if ty == "duration":
    plt.ylabel("Runtime (Seconds)", size=font_size)
  else:
    plt.ylabel("Cost ($)", size=font_size)
  fig.savefig(plot_name)
  print("Box plot", plot_name)
  plt.close()




def old_main():
  params = json.loads(open("json/smith-waterman.json").read())
  pipeline = params["pipeline"]
  filename = "dep-1533567226.383849-70"
  dependencies = json.loads(open("results/{0:s}".format(filename)).read())
  parts = filename.split("-")
  params["now"] = float(parts[1])
  params["nonce"] = int(parts[2])
  plot(dependencies, pipeline, 1, params)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--plot", type=str, required=True, help="Graph to plot")
  args = parser.parse_args()
  if args.plot == "num_spectra_comparison":
    num_spectra_comparison()
  else:
    raise Exception("Unknown plot type", args.plot)

if __name__ == "__main__":
  main()

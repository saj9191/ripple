import json
import matplotlib
import os
import time
matplotlib.use('Agg')
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes


colors = ["red", "orange", "green", "blue", "purple", "cyan", "magenta"]

def graph(key, dependencies, heights, runtimes, lefts, num_threads, parent_id, layer=0, thread_id=0):
  if thread_id >= num_threads:
    print("Sad id", thread_id)
    return thread_id

  runtimes[layer][thread_id] = float(dependencies[key]["duration"]) / 1000
  lefts[layer][thread_id] = float(dependencies[key]["timestamp"])
  parent_id = thread_id

  children = sorted(dependencies[key]["children"], key=lambda c: -1 * dependencies[c]["duration"])
  if layer == 0:
    children = [children[2]] + children[:2] + children[3:]

  for i in range(len(children)):
    child = children[i]
    thread_id = parent_id + i * max(heights[layer + 1], 0) + i
    thread_id = graph(child, dependencies, heights, runtimes, lefts, num_threads, parent_id, layer + 1, thread_id)

    if i + 1 != len(children):
      thread_id += 1
  return thread_id


def offsets(pipeline, runtimes, lefts, params):
  for layer in range(len(pipeline)):
    step = pipeline[layer]
    name = step["name"]
    parent_name = None if layer == 0 else pipeline[layer-1]["name"]
    if parent_name is not None and params["functions"][parent_name]["file"] in ["split_file", "initiate", "map"] or name in ["percolator"]:
      temp = [x + y for x, y in zip(runtimes[layer - 1], lefts[layer - 1])]
      lefts[layer] = list(map(lambda l: max(temp), lefts[layer]))
    elif params["functions"][name]["file"] in ["combine_files"] or name == "handle-specie-match":
      offset = 0
      for thread_id in range(len(lefts[layer - 1])):
        if runtimes[layer - 1][thread_id] != 0:
          offset = lefts[layer - 1][thread_id] + runtimes[layer - 1][thread_id]
        lefts[layer][thread_id] = offset
    elif layer != 0:
      for i in range(len(lefts[layer])):
        lefts[layer][i] = lefts[layer - 1][i] + runtimes[layer - 1][i]


def get_heights(key, dependencies, heights, layer = 0):
  if len(dependencies[key]["children"]) == 0:
    h = 2
  else:
    h = 0
    for child in dependencies[key]["children"]:
      h += get_heights(child, dependencies, heights, layer + 1) + 1

  if layer not in heights:
    heights[layer] = 0

  if layer > 10:
    if layer == 13:
      h = 40
    elif layer in [11, 12, 18]:
      h = 5
    elif layer == 17:
      h = 2
    else:
      h = 1 if layer == 14 else 2
    heights[layer] = h
  elif layer in [3, 4, 9]:
      h = 5
      heights[layer] = h
  else:
    heights[layer] = max(heights[layer], h)

  if layer in [4]:
    return 2
  return h


def plot(results, pipeline, params):
  num_layers = len(pipeline)

  layer_to_count = {}
  dep_file = "results/{0:f}-{1:d}/deps".format(results[0].params["now"], results[0].params["nonce"])
  dependencies = json.loads(open(dep_file).read())
  for key in dependencies.keys():
    layer = int(key.split(":")[0])
    if layer not in layer_to_count:
      layer_to_count[layer] = 0
    layer_to_count[layer] += 1

  num_threads = int(max(layer_to_count.values()) * 1.2)
  print("num_threads", num_threads)
  lefts = list(map(lambda l: [0] * num_threads, range(num_layers)))
  runtimes = list(map(lambda l: [0] * num_threads, range(num_layers)))
  threads = range(1, num_threads + 1)

  now = time.time()
  folder = "results/{0:d}-{1:f}-{2:d}".format(len(results), results[-1].params["now"], results[-1].params["nonce"])
  if not os.path.isdir(folder):
    os.makedirs(folder)
  f = open("{0:s}/runs".format(folder), "w+")

  heights = {}
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    f.write("{0:s}\n".format(dep_folder))
    dependencies = json.loads(open("results/{0:s}/deps".format(dep_folder)).read())
    root_key = list(filter(lambda k: k.startswith("0:"), dependencies.keys()))[0]
    if len(heights) == 0:
      get_heights(root_key, dependencies, heights)
    graph(root_key, dependencies, heights, runtimes, lefts, num_threads, -1, layer=0, thread_id=0)
  f.close()

  for layer in range(len(runtimes)):
    for thread_id in range(len(runtimes[layer])):
      runtimes[layer][thread_id] = float(runtimes[layer][thread_id]) / len(results)

  print("Graphing")
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  labels = []
  legends = []
  dark_colors = ["purple", "brown", "blue", "green"]
  for i in range(num_layers):
    left = lefts[i] #list(map(lambda r: float(r) / 1000, lefts[i]))
    runtime = runtimes[i]#list(map(lambda r: float(r) / 1000, runtimes[i]))
    height = 1
    if i in [14, 15, 16, 17]:
      edgecolors = list(map(lambda r: "none", runtime))
      color = dark_colors[i % len(dark_colors)]
    else:
      edgecolors = list(map(lambda r: "black" if r > 0 else "none", runtime))
      color = colors[i % len(colors)]

    height = heights[i] if heights[i] == 1 else heights[i]
    alpha=0.4
    p = ax.barh(threads, runtime, color=color, left=left, height=height, align="edge", edgecolor=edgecolors, linewidth=1, alpha=alpha)

    legends.append(p[0])
    labels.append(pipeline[i]["name"] + " ({0:d})".format(i))

  fontP = FontProperties()
  fontP.set_size('x-small')
  fig.tight_layout(rect=[0, 0, 0.85, 0.94])
  fig.legend(legends, labels, prop=fontP, loc="upper left", ncol=1, bbox_to_anchor=(0.75, 0.95))
  plt.yticks([])
 # ax.set_xlim(0, 153)
#  ax.set_ylim(0, 80)
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime ({0:d} concurrent runs)".format(len(results)))

  fig.savefig("results/test.png")


if __name__ == "__main__":
  params = json.loads(open("json/smith-waterman.json").read())
  pipeline = params["pipeline"]
  filename = "dep-1533567226.383849-70"
  dependencies = json.loads(open("results/{0:s}".format(filename)).read())
  parts = filename.split("-")
  params["now"] = float(parts[1])
  params["nonce"] = int(parts[2])
  plot(dependencies, pipeline, 1, params)

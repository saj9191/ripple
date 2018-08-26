import json
import matplotlib
from matplotlib.font_manager import FontProperties
matplotlib.use('Agg')
import matplotlib.pyplot as plt


colors = ["red", "orange", "green", "blue", "purple", "cyan", "magenta"]


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


def plot(results, pipeline, params):
  num_layers = len(pipeline)
  num_results = len(results)
  results = results[params["i"]:params["i"] + 1]
  layer_to_count = {}
  dep_file = "results/{0:f}-{1:d}/deps".format(results[0].params["now"], results[0].params["nonce"])
  dependencies = json.loads(open(dep_file).read())
  for key in dependencies.keys():
    layer = int(key.split(":")[0])
    if layer not in layer_to_count:
      layer_to_count[layer] = 0
    layer_to_count[layer] += 1

  num_threads = int(max(layer_to_count.values()) * 1)
  print("num_threads", num_threads)
  lefts = list(map(lambda l: [0] * num_threads, range(num_layers)))
  runtimes = list(map(lambda l: [0] * num_threads, range(num_layers)))
  num_offsets = list(map(lambda l: [0] * num_threads, range(num_layers)))
  threads = range(1, num_threads + 1)

  heights = {}
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    dependencies = json.loads(open("results/{0:s}/deps".format(dep_folder)).read())
    root_key = list(filter(lambda k: k.startswith("0:"), dependencies.keys()))[0]
    get_length(root_key, dependencies)
    if len(heights) == 0:
      get_heights(root_key, dependencies, heights)
    graph(root_key, dependencies, heights, num_offsets, runtimes, lefts, num_threads, -1, layer=0, thread_id=0)

  for layer in range(len(runtimes)):
    for thread_id in range(len(runtimes[layer])):
      num = max(num_offsets[layer][thread_id], 1)
      runtimes[layer][thread_id] = float(runtimes[layer][thread_id]) / num
      lefts[layer][thread_id] = float(lefts[layer][thread_id]) / num

  print("Graphing")
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
      color = colors[i % len(colors)]

    if len(heights) > i:
      height = heights[i] if heights[i] == 1 else heights[i]
    else:
      print("uhoh", i, heights)
      height = 0
    alpha = 0.4
    if i == 18:
      color = "black"
    p = ax.barh(threads, runtime, color=color, left=left, height=height, align="edge", edgecolor=edgecolors, linewidth=1, alpha=alpha)

    legends.append(p[0])
    labels.append(pipeline[i]["name"] + " ({0:d})".format(i))

  fontP = FontProperties()
  fontP.set_size('x-small')
  fig.tight_layout(rect=[0, 0, 0.85, 0.94])
  fig.legend(legends, labels, prop=fontP, loc="upper left", ncol=1, bbox_to_anchor=(0.75, 0.95))
  plt.yticks([])
  #` ax.set_xlim(120, 350)
  ax.set_ylim(0, num_threads)
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime ({0:d} concurrent runs)".format(num_results))
  plot_name = "results/concurrency{0:d}/plot-{0:d}-{1:d}.png".format(num_results, params["i"])
  print("plot", plot_name)
  fig.savefig(plot_name)
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

import json
import matplotlib
import os
import time
matplotlib.use('Agg')
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt


colors = ["red", "orange", "green", "blue", "purple", "cyan", "magenta", "brown"]


def graph(key, dependencies, runtimes, num_threads, parent_id, layer=0, thread_id=0):
  if thread_id >= num_threads:
    print("Sad id", thread_id)
    return thread_id

  runtimes[layer][thread_id] += dependencies[key]["duration"]
  parent_id = thread_id
  children = sorted(dependencies[key]["children"], key=lambda c: -1 * dependencies[c]["duration"])
  if layer == 18:
    print("tid", thread_id, "children", len(children))

  for i in range(len(children)):
    child = children[i]
    if child.startswith("1:") or child.startswith("5:"):
      thread_id = parent_id + i * 40
    elif child.startswith("10:"):
      thread_id = parent_id + i * 40
    elif child.startswith("8:"):
      thread_id = 0
    thread_id = graph(child, dependencies, runtimes, num_threads, parent_id, layer + 1, thread_id)
    if i + 1 != len(children):
      thread_id += 1
  return thread_id


def offsets(pipeline, runtimes, lefts, params):
  for layer in range(len(pipeline)):
    step = pipeline[layer]
    name = step["name"]
    parent_name = None if layer == 0 else pipeline[layer-1]["name"]
    if parent_name is not None and params["functions"][parent_name]["file"] in ["split_file", "initiate", "map"]:
      temp = [x + y for x, y in zip(runtimes[layer - 1], lefts[layer - 1])]
      lefts[layer] = list(map(lambda l: max(temp), lefts[layer]))
    elif params["functions"][name]["file"] in ["combine_files"]:
      offset = 0
      for thread_id in range(len(lefts[layer - 1])):
        if runtimes[layer - 1][thread_id] != 0:
          offset = lefts[layer - 1][thread_id] + runtimes[layer - 1][thread_id]
        lefts[layer][thread_id] = offset
    else:
      for i in range(len(lefts[layer])):
        lefts[layer][i] = lefts[layer - 1][i] + runtimes[layer - 1][i]
    if layer == 1:
      print("0", lefts[layer - 1][0:10])
      print(lefts[layer][0:10])


def plot(results, pipeline, params):
  num_layers = len(pipeline)

  layer_to_count = {}
  dependencies = json.loads(open("results/{0:f}-{1:d}/deps".format(results[0].params["now"], results[0].params["nonce"])).read())
  for key in dependencies.keys():
    layer = int(key.split(":")[0])
    if layer not in layer_to_count:
      layer_to_count[layer] = 0
    layer_to_count[layer] += 1

  num_threads = int(max(layer_to_count.values()) * 1.5)
  print("num_threads", num_threads)
  lefts = list(map(lambda l: [0] * num_threads, range(num_layers)))
  runtimes = list(map(lambda l: [0] * num_threads, range(num_layers)))
  threads = range(1, num_threads + 1)

  now = time.time()
  folder = "results/{0:d}-{1:f}".format(len(results), now)
  os.makedirs(folder)
  f = open("{0:s}/runs".format(folder), "w+")
  for result in results:
    dep_folder = "{0:f}-{1:d}".format(result.params["now"], result.params["nonce"])
    f.write("{0:s}\n".format(dep_folder))
    dependencies = json.loads(open("results/{0:s}/deps".format(dep_folder)).read())
    root_key = list(filter(lambda k: k.startswith("0:"), dependencies.keys()))[0]
    graph(root_key, dependencies, runtimes, num_threads, -1, layer=0, thread_id=0)
  f.close()

  for layer in range(len(runtimes)):
    for thread_id in range(len(runtimes[layer])):
      runtimes[layer][thread_id] = float(runtimes[layer][thread_id]) / len(results)

  print("Graphing")
  offsets(pipeline, runtimes, lefts, params)
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  labels = []
  legends = []
  for i in range(num_layers):
    left = list(map(lambda r: float(r) / 1000, lefts[i]))
    runtime = list(map(lambda r: float(r) / 1000, runtimes[i]))
    if i in [0, 3, 4]:
      height = 20
    elif i in [1, 2, 5, 6, 7, 10, 11, 13]:
      height = 10
    elif i in [14]:
      height = 1
    else:
      height = 5
    p = ax.barh(threads, runtime, color=colors[i % len(colors)], left=left, height=height, align="edge")
    legends.append(p[0])
    labels.append(pipeline[i]["name"])

  fontP = FontProperties()
  fontP.set_size('small')
  fig.tight_layout(rect=[0, 0, 0.7, 0.8])
  fig.legend(legends, labels, prop=fontP, loc="center left", ncol=1, bbox_to_anchor=(0.7, 0.5))
  plt.yticks([])
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime ({0:d} concurrent runs)".format(len(results)))
  fig.savefig("results/plot{0:d}.png".format(len(results)))


if __name__ == "__main__":
  params = json.loads(open("json/smith-waterman.json").read())
  pipeline = params["pipeline"]
  filename = "dep-1533567226.383849-70"
  dependencies = json.loads(open("results/{0:s}".format(filename)).read())
  parts = filename.split("-")
  params["now"] = float(parts[1])
  params["nonce"] = int(parts[2])
  plot(dependencies, pipeline, 1, params)

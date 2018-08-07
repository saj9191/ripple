import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


colors = ["red", "orange", "green", "blue", "purple", "cyan", "magenta"]


def graph(key, dependencies, runtimes, num_threads, parent_id, layer=0, thread_id=0):
  name = dependencies[key]["name"]
  if name in ["map-blast", "map-fasta"]:
    thread_id = 0
    parent_id = 0

  if thread_id >= num_threads:
    return thread_id

  runtimes[layer][thread_id] = max(runtimes[layer][thread_id], dependencies[key]["duration"])
  children = sorted(dependencies[key]["children"])
  parent_id = thread_id
  for i in range(len(children)):
    child = children[i]
    if dependencies[child]["name"] in ["smith-waterman"]:
      thread_id = parent_id + i * 100
    elif dependencies[child]["name"] in ["combine-blast-pivots"]:
      thread_id = parent_id + i * 10
    thread_id = graph(child, dependencies, runtimes, num_threads, parent_id, layer + 1, thread_id)
    if i + 1 != len(children):
      thread_id += 1
  return thread_id


def offsets(pipeline, runtimes, lefts):
  for layer in range(len(pipeline)):
    step = pipeline[layer]
    name = step["name"]

    if name in ["map-fasta"]:
      pass
    elif name in ["map-blast", "smith-waterman", "sort-blast-chunk"]:
      temp = [x + y for x, y in zip(runtimes[layer - 1], lefts[layer - 1])]
      lefts[layer] = list(map(lambda l: max(temp), lefts[layer]))
    elif name in ["combine-blast-files"]:
      left = None
      for i in range(len(lefts[layer])):
        if runtimes[layer - 1][i] > 0:
          left = lefts[layer - 1][i]
          runtime = runtimes[layer - 1][i]
        if left is not None:
          lefts[layer][i] = left + runtime
    else:
      for i in range(len(lefts[layer])):
        lefts[layer][i] = lefts[layer - 1][i] + runtimes[layer - 1][i]


def plot(dependencies, pipeline, iterations, params):
  num_layers = len(pipeline)
  num_threads = params["num_bins"] * 75
  lefts = list(map(lambda l: [0] * num_threads, range(num_layers)))
  runtimes = list(map(lambda l: [0] * num_threads, range(num_layers)))

  root_map_fasta_key = list(filter(lambda k: k.startswith("0:"), dependencies.keys()))[0]
  threads = range(1, num_threads + 1)
  graph(root_map_fasta_key, dependencies, runtimes, num_threads, -1, layer=0, thread_id=0)

  root_map_blast_key = list(filter(lambda k: k.startswith("4:"), dependencies.keys()))[0]
  graph(root_map_blast_key, dependencies, runtimes, num_threads, 0, layer=4, thread_id=0)

  offsets(pipeline, runtimes, lefts)
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  labels = []
  legends = []
  for i in range(num_layers):
    left = list(map(lambda r: float(r) / 1000, lefts[i]))
    runtime = list(map(lambda r: float(r) / 1000, runtimes[i]))
    if pipeline[i]["name"] in ["sort-blast-chunk", "smith-waterman", "find-blast-pivots", "combine-pivot-files"]:
      height = 75
    elif pipeline[i]["name"] in ["map-fasta", "map-blast"]:
      height = 400
    else:
      height = 2
    print(pipeline[i]["name"], height, runtime[0:20])
    p = ax.barh(threads, runtime, color=colors[i % len(colors)], left=left, height=height, align="edge")
    legends.append(p[0])
    labels.append(pipeline[i]["name"])

  fig.legend(legends, labels)
  plt.yticks([])
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime (Timestamp {0:f} Nonce {1:d})".format(params["now"], params["nonce"]))
#  fig.set_size_inches(30, 30)
  fig.savefig("results/results-{0:f}-{1:d}.png".format(params["now"], params["nonce"]))


if __name__ == "__main__":
  params = json.loads(open("json/smith-waterman.json").read())
  pipeline = params["pipeline"]
  filename = "dep-1533567226.383849-70"
  dependencies = json.loads(open("results/{0:s}".format(filename)).read())
  parts = filename.split("-")
  params["now"] = float(parts[1])
  params["nonce"] = int(parts[2])
  plot(dependencies, pipeline, 1, params)

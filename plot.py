import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot(pipeline, stats):
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  threads = range(1, 47)
  lefts = list(map(lambda l: 0.0, threads))
  colors = ["red", "orange", "green", "blue", "purple", "cyan", "magenta"]
  legends = []
  labels = []
  for i in range(len(pipeline)):
    step = pipeline[i]
    labels.append(step["name"])
    stat = stats[i]
    # TODO: Need to handle big sort
    runtimes = map(lambda d: float(d) / 1000, stat["billed_duration"][:len(threads)])
    while len(runtimes) < len(threads):
      runtimes.append(0.0)

    if step["file"] in ["combile_file"]:
      m = max(lefts)
      lefts = list(map(lambda r: m, runtimes))

    p = ax.barh(threads, runtimes, color=colors[i % len(colors)], left=lefts)
    legends.append(p[0])

    for i in range(len(runtimes)):
      lefts[i] += runtimes[i]

    if step["file"] in ["split_file"] or step["name"] in ["combine-mzML-files"]:
      m = max(lefts)
      lefts = list(map(lambda r: m, runtimes))

  plt.legend(legends, labels)
  plt.ylabel("Thread")
  plt.xlabel("Runtime (seconds)")
  plt.title("Runtime")
  plt.savefig("test.png")


if __name__ == "__main__":
  pipeline = [{
    "file": "pivot_file",
    "format": "mzML",
    "memory_size": 1024,
    "name": "find-mzML-pivots",
    "num_bins": 20,
    "output_bucket": "shjoyner-human-pivot-spectra",
    "timeout": 300
  }, {
    "file": "combine_files",
    "format": "pivot",
    "memory_size": 1024,
    "name": "combine-pivot-files",
    "input_bucket": "shjoyner-human-pivot-spectra",
    "output_bucket": "shjoyner-human-super-pivot-spectra",
    "sort": False,
    "timeout": 300
  }, {
    "file": "split_file",
    "batch_size": 4000,
    "chunk_size": 100000000,
    "format": "mzML",
    "memory_size": 1024,
    "name": "split-mzML-spectra",
    "bucket_prefix": "shjoyner-human-sort",
    "num_bins": 20,
    "input_bucket": "shjoyner-human-super-pivot-spectra",
    "output_function": "sort-mzML-chunk",
    "timeout": 300
  }]
  stats = [
   {
     "billed_duration": [10, 20, 4, 19, 5]
   },
   {
     "billed_duration": [15, 10, 9, 11, 8]
   },
   {
     "billed_duration": [12, 10, 1, 10]
   }
  ]
  plot(pipeline, stats)

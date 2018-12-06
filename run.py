import argparse
import benchmark
import boto3
import json
import math
import os
import plot
import re
import setup
import shutil
import sys
import time
import util
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class Results:
  def __init__(self, params):
    self.params = params


def clear():
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-tide")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-logs")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-ssw")
  bucket.objects.all().delete()


def get_counts(params):
  count_regex = re.compile("READ COUNT ([0-9]+) WRITE COUNT ([0-9]+) LIST COUNT ([0-9]+)")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-logs")
  layer_to_counts = {}
  READ_INDEX = 0
  WRITE_INDEX = 1
  LIST_INDEX = 2

  num_objs = 0
  for obj in bucket.objects.all():
    obj_format = util.parse_file_name(obj.key)
    layer = obj_format["prefix"]
    if layer not in layer_to_counts:
      layer_to_counts[layer] = [0, 0, 0]

    o = s3.Object("shjoyner-logs", obj.key)
    content = util.read(o, 0, o.content_length)
    m = count_regex.search(content)
    if m:
      for i in range(3):
        layer_to_counts[layer][i] += int(m.group(i + 1))
    else:
      print("Sad. Can't find count", obj.key)

    num_objs += 1
    if num_objs % 100 == 0:
      print("Processed {0:d} objects".format(num_objs))

  layers = sorted(layer_to_counts.keys())
  for layer in layers:
    print(params["pipeline"][layer - 1]["name"],
          "READ COUNT", layer_to_counts[layer][READ_INDEX],
          "WRITE COUNT", layer_to_counts[layer][WRITE_INDEX],
          "LIST COUNT", layer_to_counts[layer][LIST_INDEX]
          )


def trigger_plot(folder):
  results = []
  timestamp = time.time()
  params = json.loads(open("json/tide.json").read())
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      if params is None:
        params = json.loads(open("{0:s}/{1:s}/params".format(folder, d)).read())
      parts = d.split("-")
      now = float(parts[0])
      nonce = int(parts[1])
      p = dict(params)
      p["now"] = now
      p["nonce"] = nonce
      p["timestamp"] = timestamp
      results.append(Results(p))

  params["timestamp"] = timestamp
  params["folder"] = folder
  plot.plot(results, params["pipeline"], params)


def iterate(bucket_name, params):
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  if params["model"] == "lambda":
    setup.setup(params)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  folder = "results/{0:s}".format(params["folder"])
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  params["stats"] = True
  params["sample_input"] = True
  params["setup"] = False

  objects = list(bucket.objects.all())
  data_bucket = s3.Bucket(params["bucket"])
  for obj in objects:
    params["input_name"] = obj.key
    [upload_duration, duration, failed_attempts] = benchmark.run(params, 0)
    dir_path = "{0:s}/{1:s}".format(folder, obj.key)
    if not os.path.isdir(dir_path):
      os.makedirs(dir_path)
    data_bucket.objects.all().delete()

    with open("{0:s}/params".format(dir_path), "w+") as f:
      f.write(json.dumps(params, indent=4, sort_keys=True))

    benchmark.clear_buckets(params)


def comparison(name, lambda_folder, ec2_folder, params):
  stats = []
  lambda_duration_stats = {}
  ec2_duration_stats = {}
  for parent, folders, files in os.walk(lambda_folder):
    for folder in folders:
      f = "{0:s}/{1:s}".format(lambda_folder, folder)
      stats = []
      for subparent, subdirs, files in os.walk(f):
        if len(subdirs) > 0:
          for d in subdirs:
            s = json.load(open("{0:s}/{1:s}/stats".format(f, d)))["stats"]
            stats.append(s)
          if len(stats) > 0:
            lambda_duration_stats[folder] = get_duration_stats(stats)

  for parent, folders, files in os.walk(ec2_folder):
    for folder in folders:
      f = "{0:s}/{1:s}".format(ec2_folder, folder)
      for subparent, subdirs, files in os.walk(f):
        if len(subdirs) > 0:
          durations = []
          #costs = []
          for subdir in subdirs:
            stats = json.load(open("{0:s}/{1:s}/stats".format(f, subdir)))["stats"]
            if "methyl" in lambda_folder:
              duration = 0
              for i in range(3):
                duration += stats[i]["billed_duration"]
              for i in range(3,5):
                duration += stats[i]["billed_duration"][0]
              duration += stats[5]["billed_duration"][0] - stats[4]["billed_duration"][0]
              duration /= 1000.0
            else:
              if "ASH_20170314_HELA_120MIN.mzML" in f:
                print(f, subdir, stats[-2]["max_duration"] / 1000.0)
              duration = stats[-2]["max_duration"] / 1000.0
            # costs.append((duration * memory["ec2"][params["ec2"]["type"]]) / 3600)
            durations.append(duration)
          ec2_duration_stats[folder] = durations
          #ec2_cost_stats[folder] = costs

  lambda_duration = sum(list(map(lambda s: sum(s), lambda_duration_stats.values())))
  lambda_count = sum(list(map(lambda s: len(s), lambda_duration_stats.values())))
#  lambda_cost = sum(list(map(lambda s: sum(s), lambda_cost_stats.values())))
  ec2_duration = sum(list(map(lambda s: sum(s), ec2_duration_stats.values())))
  ec2_count = sum(list(map(lambda s: len(s), ec2_duration_stats.values())))
#  ec2_cost = sum(list(map(lambda s: sum(s), ec2_cost_stats.values())))

  print("Average Lambda Duration", lambda_duration / lambda_count)
#  print("Average Lambda Cost", lambda_cost / lambda_count)
  print("Average EC2 Duration", ec2_duration / ec2_count)
#  print("Average EC2 Cost", ec2_cost / ec2_count)
  plot.statistics(name, lambda_folder, [lambda_duration_stats, ec2_duration_stats], ["Lambda", "EC2"], "duration")
#  plot.statistics(name, lambda_folder, lambda_cost_stats, ec2_cost_stats, "cost")


def get_duration_stats(stats):
  layers_to_duration = {}
  layers_to_count = {}
  regions = []
  c = -1
  for pipeline in stats:
    c += 1
    result_regions = {}
    start_time = None
    duration = 0
    for i in range(len(pipeline)):
      stat = pipeline[i]
      layer = i - 1
      name = stat["name"]
      if name not in ["load", "total"]:
        if layer not in layers_to_duration:
          layers_to_duration[layer] = {"timestamp": 0, "duration": 0}
          layers_to_count[layer] = 0
        if layer not in result_regions:
          result_regions[layer] = [sys.maxsize, 0]

        for message in stat["messages"]:
          jmessage = json.loads(message)
          if layer == 0:
            if start_time is None:
              start_time = jmessage["start_time"]
            else:
              start_time = min(start_time, jmessage["start_time"])
          start = jmessage["start_time"] - start_time
          end = start + math.ceil(jmessage["duration"] / 1000)
          duration = max(duration, end)
          result_regions[layer][0] = min(result_regions[layer][0], start)
          result_regions[layer][1] = max(result_regions[layer][1], end)

          layers_to_duration[layer]["timestamp"] += jmessage["start_time"]
          layers_to_duration[layer]["duration"] += jmessage["duration"]
          layers_to_count[layer] += 1

    regions.append(result_regions)

  durations = []
  max_layer = len(regions[0]) - 1
  for region in regions:
    duration = region[max_layer][1]
    durations.append(duration)

  return durations


def get_cost_stats(stats, folder, params):
  memory = json.loads(open("json/memory.json").read())
  s3_costs = list(map(lambda s: 0, stats))
  costs = list(map(lambda s: 0, stats))

  for sindex in range(len(stats)):
    pipeline = stats[sindex]
    start_time = None
    duration = 0
    for i in range(len(pipeline)):
      stat = pipeline[i]
      layer = i - 1
      name = stat["name"]
      if name not in ["load", "total"]:
        for message in stat["messages"]:
          jmessage = json.loads(message)
          if layer == 0:
            if start_time is None:
              start_time = jmessage["start_time"]
            else:
              start_time = min(start_time, jmessage["start_time"])
          start = jmessage["start_time"] - start_time
          end = start + math.ceil(jmessage["duration"] / 1000)
          duration = max(duration, end)
          function_name = params["pipeline"][layer]["name"]
          memory_size = str(params["functions"][function_name]["memory_size"])
          duration_cost = int(jmessage["duration"] / 100) * memory["lambda"][memory_size]
          costs[sindex] += duration_cost
          s3_costs[sindex] += (jmessage["read_count"] / 1000) * 0.0004
          s3_costs[sindex] += (jmessage["write_count"] / 1000) * 0.005
          s3_costs[sindex] += (jmessage["list_count"] / 1000) * 0.005
          s3_costs[sindex] += (jmessage["byte_count"] / 1024 / 1024 / 1024) * 0.023
        #    print(folder, "read count", jmessage["read_count"])
        #    print(folder, "write count", jmessage["write_count"])
        #    print(folder, "list count", jmessage["list_count"])
        #    print(folder, "byte count", jmessage["byte_count"])

  return costs, s3_costs


def get_start_time(stats):
  start_times = []
  for pipeline in stats:
    for i in range(len(pipeline)):
      stat = pipeline[i]
      if type(stat) == list:
        stat = stat[0]
      name = stat["name"]
      if name not in ["load", "total"]:
        for message in stat["messages"]:
          jmessage = json.loads(message)
          start_times.append(jmessage["start_time"])
  return min(start_times)


def get_lambda_results(folder, params, concurrency=None, absolute=False, li=None):
  stats = []
  e = None
  ds = []
  for subdir, dirs, files in os.walk(folder):
    ds += dirs

  ds.sort()
  print("ds", len(ds))
  for d in ds:
    assert(e is None or e < d)
    e = d
    file_name = "{0:s}/{1:s}/stats".format(folder, d)
    s = json.load(open(file_name))["stats"]
    stats.append(s)
  points = []
  start_time = get_start_time(stats)
  times = {}
#  print("first", json.loads(stats[0][1]["messages"][0])["start_time"])
#  print("second", json.loads(stats[1][1]["messages"][0])["start_time"])
  for s_index in range(len(stats)):
    pipeline = stats[s_index]
    t = len(times)
    times[t] = [sys.maxsize, 0]
    for i in range(len(pipeline)):
      stat = pipeline[i]
      if type(stat) == list:
        stat = stat[0]
      layer = i - 1

      name = stat["name"]
      if name not in ["load", "total"]:
        for message in stat["messages"]:
          jmessage = json.loads(message)
          if layer == 0 and not absolute:
            start_time = jmessage["start_time"]
          start = jmessage["start_time"] - start_time
          end = start + math.ceil(jmessage["duration"] / 1000)
          times[t][0] = min(times[t][0], start)
          times[t][1] = max(times[t][1], end)
          points.append([start, 1, layer, s_index])
          points.append([end, -1, layer, s_index])

  points.sort()
  layer_to_count = {}
  num_layers = len(params["pipeline"])
  layer_to_x = {num_layers: []}
  layer_to_y = {num_layers: []}
  layer_to_count[num_layers] = 0

  for [x, c, layer, job] in points:
    if layer not in layer_to_x:
      layer_to_count[layer] = 0
      layer_to_x[layer] = []
      layer_to_y[layer] = []
    layer_to_count[layer] += c
    layer_to_count[num_layers] += c
    assert(layer_to_count[layer] <= layer_to_count[num_layers])
    layer_to_x[layer].append(x)
    layer_to_x[num_layers].append(x)
    count = float(layer_to_count[layer]) / len(stats) if concurrency is None else layer_to_count[layer]
    layer_to_y[layer].append(count)
    count = float(layer_to_count[num_layers]) / len(stats) if concurrency is None else layer_to_count[num_layers]
    layer_to_y[num_layers].append(count)

  print(len(layer_to_x), len(points))
  return [layer_to_x, layer_to_y, times, points]


def get_ec2_results(folder):
  stats = []
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats.append(json.load(open(file_name))["stats"])

  layers_to_averages = {}
  layers_to_cost = {}
  layers_to_names = {}

  timestamps = []

  for stat in stats:
    end_time = stat[0]["end_time"]
    start_time = end_time - stat[-1]["max_duration"] / 1000
    timestamps.append([start_time, end_time])
    layer = 0
    for s in stat:
      if "name" not in s:
        s["name"] = "termination"
      layers_to_names[layer] = s["name"]
      if s["name"] not in ["load", "total"]:
        if layer not in layers_to_averages:
          layers_to_averages[layer] = 0
          layers_to_cost[layer] = 0
        print(s["name"], s["max_duration"])
        layers_to_averages[layer] += s["max_duration"]
        layers_to_cost[layer] += s["cost"]
        layer += 1

  start_time = min(list(map(lambda x: x[0], timestamps)))
  points = []
  for timestamp in timestamps:
    st = timestamp[0] - start_time
    et = timestamp[1] - start_time
    points.append([st, 1])
    points.append([et, -1])
  points.sort()
  layer_to_x = []
  layer_to_y = []

  count = 0
  for [x, c] in points:
    layer_to_x.append(x-1)
    layer_to_y.append(count)
    count += c
    layer_to_x.append(x)
    layer_to_y.append(count)

  average_cost = 0
  average_duration = 0
  for layer in layers_to_averages.keys():
    layers_to_averages[layer] /= (len(stats) * 1000)
    layers_to_cost[layer] /= len(stats)
    average_cost += layers_to_cost[layer]
    average_duration += layers_to_averages[layer]

  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(layer_to_x, layer_to_y)
  plt.xlabel("Runtime (seconds)")
  plot_name = "{0:s}/{1:s}.png".format(folder, "ec2-uniform")
  print(plot_name)
  fig.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()

  return [layers_to_averages, layers_to_cost, layer_to_x, layer_to_y]


def render_image(name, folder, params):
  render(name, folder, params, compare=True, concurrency=None, absolute=False)


def accumulation():
  # comparison("Tide", "results/tide-files-lambda", "results/tide-files-ec2", params)
  # cdf(folder, times, labels):
  #  render("Spacenet", "knn-deadline", "results/knn-deadline/3band_AOI_1_RIO_img5792.tif", "", params, concurrency=2, absolute=True)
  #  render("Spacenet", "knn", "results/knn100", "", params, concurrency=100, absolute=True)
  #  params = json.loads(open("json/tide.json").read())
  #  render("Methyl DNA Decompression", "decompression", "results/decompression", "", params, concurrency=None, absolute=False)
  #  file_average.render("Tide", "tide", "tide", params)
  #  render("Spacenet", "knn", "results/knn/3band_AOI_1_RIO_img5792.tif", "", params, concurrency=None, absolute=False)
  #  params = json.loads(open("json/private-smith-waterman.json").read())
  #  comparison("Smith Waterman", "results/ssw-files-lambda", "results/ssw-files-ec2", params)
  # params = json.loads(open("json/private-compression.json").read())
  # comparison("Methyl DNA Decompression", "results/methyl-files-lambda", "results/methyl-files-ec2", params)
#  render("Tide", "results/tide-bursty-lambda", params, concurrency=100, absolute=True)
#  absolute = True
  params = json.loads(open("json/private-smith-waterman.json").read())
  prefix = "ssw"
  concurrency = 100
  absolute = True
  [x, y, fifo_times, points] = get_lambda_results("results/" + prefix + "100", params, concurrency, absolute)
  [x, y, robin_times, points] = get_lambda_results("results/" + prefix + "100-robin-2", params, concurrency, absolute)
  plot.cdf("results/" + prefix + "100", [robin_times, fifo_times], ["Round Robin", "FIFO"])


def render(name, lambda_folder, params, compare=True, concurrency=None, absolute=False):
  [x, y, times, points] = get_lambda_results(lambda_folder, params, concurrency, absolute)

  plot_name = "{name}_accumulation".format(name=name)

  plot.accumulation_plot(
      x,
      y,
      points,
      params["pipeline"],
      plot_name,
      lambda_folder,
      absolute
  )

  # plot_name = "{name}_duration".format(name=name)
  # plot.durations(times, lambda_folder, plot_name)
  return


def regularize():
  folder = "results/tide100"
  for subdir, dirs, files in os.walk(folder):
    for d in dirs:
      file_name = "{0:s}/{1:s}/stats".format(folder, d)
      s = json.load(open(file_name))["stats"]
      stats = []
      for ss in s:
        if type(ss) == dict:
          assert("name" in ss)
          stats.append(ss)
        elif type(ss) == list:
          assert(len(ss) == 1)
          stats.append(ss[0])
      assert(len(stats) == 10)
      if not os.path.isdir("results/test-tide100/{0:s}".format(d)):
        os.mkdir("results/test-tide100/{0:s}".format(d))
        with open("results/test-tide100/{0:s}/stats".format(d), "w+") as f:
          f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))


def move():
  folder = "results/tide100"
  for subdir, dirs, files in os.walk(folder):
    if len(dirs) > 0 and "." in subdir:
      for d in dirs:
        if os.path.isdir(folder + "/" + d):
          print("Sad", d)
        else:
          shutil.move(subdir + "/" + d, folder + "/" + d)
      shutil.rmtree(subdir)


def concurrency():
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-log")
  params = json.load(open("json/tide.json"))
  for obj in bucket.objects.filter(Prefix="1/"):
    stats = []
    token = obj.key.split("/")[1]
    parts = token.split("-")
    params["now"] = float(parts[0])
    params["nonce"] = int(parts[1])
    benchmark.parse(stats, params)
    dir_path = "results/{0:s}/{1:f}-{2:d}".format(params["folder"], params["now"], params["nonce"])
    if not os.path.isdir(dir_path):
      os.makedirs(dir_path)
    with open("{0:s}/stats".format(dir_path), "w+") as f:
      f.write(json.dumps({"stats": stats}, indent=4, sort_keys=True))


def download(params, token, folder):
  [timestamp, nonce] = token.split("-")
  params["now"] = float(timestamp)
  params["nonce"] = int(nonce)
  stats = benchmark.parse_logs(params, params["now"], 0, 0)
  dir_path = "results/{0:s}/{1:s}/{2:f}-{3:d}".format(folder, params["input_name"], params["now"], params["nonce"])
  os.makedirs(dir_path)
  with open("{0:s}/stats".format(dir_path), "w+") as f:
    f.write(json.dumps({"stats": stats, "failed": False}, indent=4, sort_keys=True))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--clear', action="store_true", help="Clear bucket files")
  parser.add_argument('--plot', type=str, help="Plot graph")
  parser.add_argument('--counts', action="store_true", help="Get read / write counts")
  parser.add_argument('--parameters', type=str, help="JSON parameter file to use")
  parser.add_argument('--iterate', type=str, help="Bucket to iterate through")
  parser.add_argument('--accumulation', action="store_true", help="Plot accumulation graph")
  parser.add_argument('--comparison', action="store_true", help="Plot comparison graph")
  parser.add_argument('--folder', type=str, help="Folder to store results in")
  parser.add_argument('--download', action="store_true", default=False)
  parser.add_argument('--render', action="store_true", default=False)
  parser.add_argument('--name', type=str, help="Prefix for file names")
  parser.add_argument('--token', type=str)
  args = parser.parse_args()

  params = {}
  if args.parameters:
    params = json.load(open(args.parameters))
    if len(args.folder) > 0:
      params["folder"] = args.folder

  if args.render:
    render_image(args.name, args.folder, params)
  if args.accumulation:
    accumulation()
  if args.comparison:
    comparison()
  if args.clear:
    clear()
  if args.plot:
    trigger_plot(args.plot)
  if args.counts:
    get_counts(params)
  if args.iterate:
    iterate(args.iterate, params)
  if args.download:
    download(params, args.token, args.folder)


if __name__ == "__main__":
  # concurrency()
  # move()
  main()

  # comparison("Tide", "results/tide-files-lambda", "results/tide-files-ec2", json.load(open("json/ec2-tide.json")))
  # comparison("Smith Waterman", "results/ssw-files-lambda", "results/ssw-files-ec2", json.load(open("json/ec2-smith-waterman.json")))

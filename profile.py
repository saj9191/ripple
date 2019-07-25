import clear
from collections import defaultdict
from database.s3 import S3
import json
import os
import queue
import random
import ripple
import statistics
import threading
import time
import upload
import util


# TODO: Profile memory size changes
split_sizes = [
   1*1000*1000,
   10*1000*1000,
   20*1000*1000,
   50*1000*1000,
  100*1000*1000,
]

memory_sizes = [
#  256,
#  512,
  1024,
  2240,
  3008
]

regions = [
  "us-east-1",
#  "us-east-2",
  "us-west-1",
  "us-west-2",
]

done_queue = queue.Queue()
log_queue = queue.Queue()
num_iterations = 3

class TimeoutError(Exception):
  pass

def setup(table, log, json_name, split_size, mem_size, region):
  config = {
    "region": region,
    "role": "service-role/lambdaFullAccessRole",
    "memory_size": 3008
  }

  pipeline = ripple.Pipeline(name="tide", table="s3://" + table, log="s3://" + log, timeout=600, config=config)
  input = pipeline.input(format="mzML")
  step = input.split({"split_size": split_size}, {"memory_size": 128})

  params={
    "database_bucket": "maccoss-fasta",
    "num_threads": 0,
    "species": "normalHuman",
  }
  step = input.run("tide", params=params, output_format="tsv", config={"memory_size": mem_size})
  step = step.combine(params={"sort": False}, config={"memory_size": 256})
  params={
    "database_bucket": "maccoss-fasta",
    "max_train": 10*1000,
    "output": "peptides",
  }
  step = step.run("percolator", params=params)
  pipeline.compile(json_name, dry_run=False)
  return len(pipeline.pipeline)


def wait_for_execution_to_finish(db, table, key, num_steps):
  entries = []
  m = util.parse_file_name(key) 
  m["prefix"] = num_steps
  prefix = util.key_prefix(util.file_name(m))
  start_time = time.time()
  while len(entries) == 0:
    entries = db.get_entries(table, prefix)
    if time.time() - start_time > num_steps * 60 * 2:
      print("Tiemout", num_steps * 60 * 2)
      raise TimeoutError
    time.sleep(10)


def profile(region):
  db = S3({})
  dir_path = os.path.dirname(os.path.realpath(__file__))
  suffix = "-".join(region.split("-")[1:])
  table = "maccoss-tide-" + suffix
  log = "maccoss-log-" + suffix
  source_bucket = "tide-source-data"
  json_name = "json/basic-tide-" + suffix +".json"
  if region != "us-west-2":
    source_bucket += "-" + suffix
 
  entries = db.get_entries(source_bucket)
  random.shuffle(entries)

  i = 0
  for it in range(num_iterations):
    for split_size in split_sizes:
      print(region, "Profiling split size", split_size)
      for mem_size in memory_sizes:
        print(region, "Profiling mem size", mem_size)
        entry  = entries[i % len(entries)]
        name = entry.key
        size = entry.content_length()
        num_steps = setup(table, log, json_name, split_size, mem_size, region)
        key, _, _ = upload.upload(table, name, source_bucket)
        token = key.split("/")[1]
        try:
          wait_for_execution_to_finish(db, table, key, num_steps)
          params = json.loads(open(dir_path + "/" + json_name).read())
          stats, costs, durations = statistics.statistics(log, token, None, params, None)
          duration = durations[-1][1] - durations[-1][0]
          cost = costs[-1]
          m = f"{name},{size},{region},{split_size},{mem_size},{duration},{cost}\n"
        except TimeoutError:
          duration = -1.0
          cost = -1.0
          m = f"{name},{size},{region},{split_size},{mem_size},{duration},{cost}\n"
        print(m)
        log_queue.put(m)
        clear.clear(table, token, None)
        clear.clear(log, token, None)
        i += 1
  done_queue.put("done")

def averages(total, count):
  average = {}
  for key in total.keys():
    average[key] = [total[key][0] / count[key], total[key][1] / count[key]]
  return average

def best(average):
  print(average)
  best_cost = sorted(average.keys(), key=lambda k: average[k][0])[0]
  best_duration = sorted(average.keys(), key=lambda k: average[k][1])[0]
  print("Best cost", best_cost, average[best_cost][0], "... Duration", average[best_cost][1])
  print("Best duration", best_duration, average[best_duration][1], "... Cost", average[best_duration][0])

def process(file_name, agg):
  total_with = defaultdict(lambda: [0, 0])
  total_without = defaultdict(lambda: [0, 0])
  with_count = defaultdict(lambda: 0)
  without_count = defaultdict(lambda: 0)

  with open(file_name, "r") as f:
    lines = f.readlines()[1:]
  for line in lines[1:]:
    parts = line.split(",")
    try:
      if agg == "":
        with_key = ",".join(parts[0:4])
        without_key = parts[0] + "," + ",".join(parts[2:4])
      elif agg == "size":
        agg_size = 1000*1000
        size = int((int(parts[1]) / agg_size)) * agg_size
        without_key = str(size) + "," + ",".join(parts[3:5])
        with_key = without_key + "," + parts[2]

      duration = float(parts[5])
      cost = float(parts[6])
    except:
      continue
 
    if duration < 0:
      duration = float("inf")
      cost = float("inf")
    total_with[with_key][0] += cost
    total_without[without_key][0] += cost
    total_with[with_key][1] += duration
    total_without[without_key][1] += duration
    without_count[without_key] += 1
    with_count[with_key] += 1

  avg_with = averages(total_with, with_count)
  avg_without = averages(total_without, without_count)

  print("With Region")
  best(avg_with)
  print("")
  print("Without Region")
  best(avg_without)
  return avg_with, avg_without

def run():
  threads = []
  for region in regions:
    threads.append(threading.Thread(target=profile, args=(region,)))
    threads[-1].start()

  dir_path = os.path.dirname(os.path.realpath(__file__))
  file_name = dir_path + "/profile.csv"

  with open(file_name, "a+") as f:
    f.write("Name,Size,Region,Split Size,Memory Size,Total Duration,Total Cost\n")
    while done_queue.qsize() < len(threads):
      while not log_queue.empty():
        f.write(log_queue.get())
        f.flush()
      time.sleep(30)

    for thread in threads:
      thread.join()
    time.sleep(10)
    while not log_queue.empty():
      f.write(log_queue.get())
      f.flush()

  process(file_name, "")


if __name__== "__main__":
  run()

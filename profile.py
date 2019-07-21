import clear
from collections import defaultdict
from database.s3 import S3
import json
import os
import queue
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

num_iterations = 3

done_queue = queue.Queue()
log_queue = queue.Queue()

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
  print(table, prefix)
  while len(entries) == 0:
    entries = db.get_entries(table, prefix)
    if time.time() - start_time > num_steps * 60 * 2:
      print("Tiemout", num_steps * 60 * 2)
      raise Exception("Timeout")
    time.sleep(10)


def profile(region):
  db = S3({})
  dir_path = os.path.dirname(os.path.realpath(__file__))
  name = "PXD005709/150130-15_0321-01-AKZ-F01.mzML"
  suffix = "-".join(region.split("-")[1:])
  table = "maccoss-tide-" + suffix
  log = "maccoss-log-" + suffix
  source_bucket = "tide-source-data"
  json_name = "json/basic-tide-" + suffix +".json"
  if region != "us-west-2":
    source_bucket += "-" + suffix

  for split_size in split_sizes:
    print(region, "Profiling split size", split_size)
    for mem_size in memory_sizes:
      print(region, "Profiling mem size", mem_size)
      num_steps = setup(table, log, json_name, split_size, mem_size, region)
      for iteration in range(num_iterations):
        print("Profile iteration", iteration)
        key, _, _ = upload.upload(table, name, source_bucket)
        token = key.split("/")[1]
        try:
          wait_for_execution_to_finish(db, table, key, num_steps)
          params = json.loads(open(dir_path + "/" + json_name).read())
          stats, costs, durations = statistics.statistics(log, token, None, params, None)
          duration = durations[-1][1] - durations[-1][0]
          cost = costs[-1]
          m = f"{name},{region},{split_size},{mem_size},{iteration},{duration},{cost}\n"
        except Exception as e:
          print(e)
          duration = -1.0
          cost = -1.0
          m = f"{name},{region},{split_size},{mem_size},{iteration},{duration},{cost}\n"
        print(m)
        log_queue.put(m)
        clear.clear(table, token, None)
        clear.clear(log, token, None)
  done_queue.put("done")

def averages(cost, duration, count):
  average_cost = {}
  average_duration = {}
  for key in duration.keys():
    average_cost[key] = cost[key] / count[key]
    average_duration[key] = duration[key] / count[key]
  return average_cost, average_duration

def best(cost, duration):
  best_cost = sorted(cost.keys(), key=lambda k: cost[k])[0]
  best_duration = sorted(duration.keys(), key=lambda k: duration[k])[0]
  print("Best cost", best_cost, cost[best_cost], "... Duration", duration[best_cost])
  print("Best duration", best_duration, duration[best_duration], "... Cost", cost[best_duration])

def process(file_name):
  total_with_cost = defaultdict(lambda: 0)
  total_without_cost = defaultdict(lambda: 0)
  total_with_duration = defaultdict(lambda: 0)
  total_without_duration = defaultdict(lambda: 0)
  with_count = defaultdict(lambda: 0)
  without_count = defaultdict(lambda: 0)

  with open(file_name, "r") as f:
    lines = f.readlines()[1:]
  for line in lines[1:]:
    parts = line.split(",")
    with_key = ",".join(parts[0:4])
    without_key = parts[0] + "," + ",".join(parts[2:4])

    duration = float(parts[5])
    cost = float(parts[6])
    if duration < 0:
      duration = float("inf")
      cost = float("inf")
    total_with_duration[with_key] += duration
    total_without_duration[without_key] += duration
    total_with_cost[with_key] += cost
    total_without_cost[without_key] += cost
    without_count[without_key] += 1
    with_count[with_key] += 1

  avg_with_cost, avg_with_duration = averages(total_with_cost, total_with_duration, with_count)
  avg_without_cost, avg_without_duration = averages(total_without_cost, total_without_duration, without_count)

  print("With Region")
  best(avg_with_cost, avg_with_duration)
  print("")
  print("Without Region")
  best(avg_without_cost, avg_without_duration)

def run():
  threads = []
  for region in regions:
    threads.append(threading.Thread(target=profile, args=(region,)))
    threads[-1].start()

  with open("profile.csv", "a+") as f:
    f.write("Name,Region,Split Size,Memory Size,Iteration,Total Duration,Total Cost\n")
    while done_queue.qsize() < len(threads):
      while not log_queue.empty():
        f.write(log_queue.get())
        f.flush()
      time.sleep(30)

    for thread in threads:
      thread.join()


process("profile.csv")
#run()

import clear
from database.s3 import S3
import json
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
  "us-east-2",
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
  while len(entries) == 0:
    entries = db.get_entries(table, prefix)
    if time.time() - start_time > num_steps * 60 * 2:
      raise Exception("Timeout")
    time.sleep(10)


def profile(region):
  db = S3({})
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
          params = json.loads(open("../json/" + json_name).read())
          stats, costs, durations = statistics.statistics(log, token, None, params, None)
          duration = durations[-1][1] - durations[-1][0]
          cost = costs[-1]
          log_queue.put("{name},{region},{split_size},{mem_size},{iteration},{duration},{cost}\n")
        except Exception:
          duration = -1.0
          cost = -1.0
          log_queue.put("{name},{region},{split_size},{mem_size},{iteration},{duration},{cost}\n")
        clear.clear(table, token, None)
        clear.clear(log, token, None)
  done_queue.put("done")


def process(file_name):
  total_costs = {}
  total_durations = {}
  with open(file_name, "r") as f:
    lines = f.readlines()[1:]
    for line in lines:
      parts = line.split(",")
      key = ",".join(parts[:2])
      values = list(map(lambda p: float(p), parts[2:]))
      if key not in total_costs:
        total_costs[key] = 0.0
        total_durations[key] = 0.0
      if values[1] == -1:
        total_durations[key] += float("inf")
        total_costs[key] += float("inf")
      else:
        total_durations[key] += values[1]
        total_costs[key] += values[2]

  average_costs = {}
  average_durations = {}
  for key in total_durations.keys():
    average_costs[key] = total_costs[key] / 10.0
    average_durations[key] = total_durations[key] / 10.0

  best_cost = sorted(average_costs.keys(), key=lambda k: average_costs[k])[0]
  best_duration = sorted(average_costs.keys(), key=lambda k: average_durations[k])[0]
  print("Best cost", best_cost, average_costs[best_cost], "... Duration", average_durations[best_cost])
  print("Best duration", best_duration, average_durations[best_duration], "... Cost", average_costs[best_duration])


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
      time.sleep(30)

    for thread in threads:
      thread.join()


#process("profile.csv.1")
run()

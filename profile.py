import clear
from database.s3 import S3
import json
import ripple
import statistics
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

num_iterations = 10

def setup(split_size, mem_size):
  print("Compiling...")
  config = {
    "region": "us-west-2",
    "role": "service-role/lambdaFullAccessRole",
    "memory_size": 3008
  }

  pipeline = ripple.Pipeline(name="tide", table="s3://maccoss-tide", log="s3://maccoss-log", timeout=600, config=config)
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
  pipeline.compile("json/basic-tide.json", dry_run=False)
  print("Compiled")
  return len(pipeline.pipeline)


def wait_for_execution_to_finish(db, key, num_steps):
  entries = []
  m = util.parse_file_name(key) 
  m["prefix"] = num_steps
  prefix = util.key_prefix(util.file_name(m))
  print("Waiting for prefix", prefix)
  start_time = time.time()
  while len(entries) == 0:
    entries = db.get_entries("maccoss-tide", prefix)
    if time.time() - start_time > num_steps * 60 * 2:
      raise Exception("Timeout")
    time.sleep(30)


def profile(f):
  db = S3({})
  f.write("Split Size,Memory Size,Iteration,Total Duration,Total Cost\n")
  for split_size in split_sizes:
    print("Profiling split size", split_size)
    for mem_size in memory_sizes:
      print("Profiling mem size", mem_size)
      num_steps = setup(split_size, mem_size)
      for iteration in range(num_iterations):
        print("Profile iteration", iteration)
        key, _, _ = upload.upload("maccoss-tide", "PXD005709/150130-15_0321-01-AKZ-F01.mzML", "tide-source-data")
        token = key.split("/")[1]
        try:
          wait_for_execution_to_finish(db, key, num_steps)
          params = json.loads(open("../json/basic-tide.json").read())
          stats, costs, durations = statistics.statistics("maccoss-log", token, None, params, None)
          duration = durations[-1][1] - durations[-1][0]
          f.write("{0:d},{1:d},{2:d},{3:f},{4:f}\n".format(split_size, mem_size, iteration, duration, costs[-1]))
        except Exception:
          f.write("{0:d},{1:d},{2:d},{3:f},{4:f}\n".format(split_size, mem_size, iteration, -1.0, -1.0), flush)
        f.flush()
        clear.clear("maccoss-tide", token, None)
        clear.clear("maccoss-log", token, None)


def run():
  with open("profile.csv", "w+") as f:
    profile(f)

run()

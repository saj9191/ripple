import argparse
import inspect
import json
import matplotlib
import os
import sys
matplotlib.use('Agg')
import matplotlib.pyplot as plt
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/ec2")
import graph


def process(subfolder):
  token_to_counts = {}
  min_start_time = None
  stage_to_token_to_range = {}
  stage_to_token_to_count = {}
  stage_to_token_to_duration = {}
  for f in os.listdir(subfolder):
    if f.endswith(".log"):
      body = json.loads(open(subfolder + "/" + f, "r").read())
      start_time = body["start_time"]
      min_start_time = min([start_time, min_start_time]) if min_start_time else start_time

  for f in os.listdir(subfolder):
    if f.endswith(".log"):
      body = json.loads(open(subfolder + "/" + f, "r").read())
      parts = f.split(".")
      stage = int(parts[0])
      if stage not in stage_to_token_to_range:
        stage_to_token_to_range[stage] = {}
        stage_to_token_to_count[stage] = {}
        stage_to_token_to_duration[stage] = {}
      token = ".".join(parts[1:3])
      if token not in stage_to_token_to_range[stage]:
        stage_to_token_to_range[stage][token] = [sys.maxsize, 0]
        stage_to_token_to_count[stage][token] = 0
        stage_to_token_to_duration[stage][token] = 0
      if token not in token_to_counts:
        token_to_counts[token] = []
      start_time = body["start_time"] - min_start_time
      assert(start_time >= 0)
      end_time = start_time + body["duration"] / 1000.0 - 1
      stage_to_token_to_range[stage][token][0] = min(stage_to_token_to_range[stage][token][0], start_time)
      stage_to_token_to_range[stage][token][1] = max(stage_to_token_to_range[stage][token][1], end_time)
      stage_to_token_to_count[stage][token] += 1
      stage_to_token_to_duration[stage][token] += (end_time - start_time)
      token_to_counts[token].append([start_time, 1])
      token_to_counts[token].append([end_time, -1])

  for token in stage_to_token_to_range[0].keys():
    for stage in range(len(stage_to_token_to_range)):
      print("Stage", stage, "Token", token, stage_to_token_to_range[stage][token][0], stage_to_token_to_range[stage][token][1], stage_to_token_to_count[stage][token])
      if stage_to_token_to_count[stage][token] != 0:
        print("Average Duration", stage_to_token_to_duration[stage][token] / stage_to_token_to_count[stage][token])
      else:
        print("Average Duration", 0)

  numbers = []
  for token in token_to_counts.keys():
    counts = token_to_counts[token]
    counts = sorted(counts, key=lambda c: c[0])
    ranges = []
    num_functions = 0
    max_num = 0
    for i in range(len(counts)):
      ranges.append([counts[i][0] - 1, num_functions])
      num_functions += counts[i][1]
      max_num = max(num_functions, max_num)
      ranges.append([counts[i][0], num_functions])
    print(token, max_num)
    numbers.append(ranges)

  colors = ["red", "blue", "gray", "purple", "green", "orange", "blue", "cyan", "pink", "brown"]
  graph.graph(subfolder, numbers, colors, None, None, None)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--subfolder", type=str, required=True, help="Subfolder with simulation data")
  args = parser.parse_args()
  process(args.subfolder)


main()

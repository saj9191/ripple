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
  statistics = json.loads(open(subfolder + "/statistics").read())["stats"]
  token_to_counts = {}
  min_start_time = None
  for stage in range(len(statistics)):
    for message in statistics[stage]["messages"]:
      token = message["log"].split("/")[1]
      if token not in token_to_counts:
        token_to_counts[token] = []
      body = message["body"]
      start_time = body["start_time"]
      min_start_time = min([start_time, min_start_time]) if min_start_time else start_time
      end_time = start_time + body["duration"] / 1000.0
      token_to_counts[token].append([start_time, 1])
      token_to_counts[token].append([end_time, -1])

  numbers = []
  labels = []
  for token in token_to_counts.keys():
    counts = list(map(lambda c: [c[0] - min_start_time, c[1]], token_to_counts[token]))
    counts = sorted(counts, key=lambda c: c[0])
    ranges = []
    num_functions = 0
    for i in range(len(counts)):
      ranges.append([counts[i][0] - 1, num_functions])
      num_functions += counts[i][1]
      ranges.append([counts[i][0], num_functions])
    print("ranges", ranges)
    numbers.append(ranges)
    labels.append(token)

  colors = ["red", "blue", "gray", "purple", "green", "orange", "blue", "cyan", "pink", "brown"]
  graph.graph(subfolder, numbers, colors, labels, None, None)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--subfolder", type=str, required=True, help="Subfolder with simulation data")
  args = parser.parse_args()
  process(args.subfolder)


main()

import argparse
import inspect
import json
import os
import sys
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/ec2")
import graph


def sort(ranges, start_time):
  results = []
  ranges.sort()
  num = 0
  maxnum = 0
  for i in range(len(ranges)):
     results.append([ranges[i][0] - start_time - 1, num])
     num += ranges[i][1]
     maxnum = max(num, maxnum)
     results.append([ranges[i][0] - start_time, num])
  return results


def parse(subfolder):
  applications = json.loads(open(subfolder + "/statistics").read())
  function_ranges = []
  application_ranges = []
  min_time = sys.maxsize

  total_job_time = 0
  vcpus = (2*1536.0)/3008
  print("vcpus", vcpus)
  et1 = sys.maxsize
  et2 = 0
  for points in applications:
    start_time = sys.maxsize
    end_time = 0
    stages = {}
    for point in points:
      start_time = min(start_time, point[0])
      if point[0] not in stages:
        stages[point[0]] = 0
      stages[point[0]] += 1
      end_time = max(end_time, point[1])
      min_time = min(min_time, point[0])
      et1 = min(et1, end_time)
      et2 = max(et2, end_time)
      function_ranges.append([point[0], vcpus])
      function_ranges.append([point[1], -1 * vcpus])
    application_ranges.append([start_time, 1])
    application_ranges.append([end_time, -1])
    total_job_time += (end_time - start_time)

  print("min_time", min_time)
  print("Average Job Time", total_job_time / len(applications))
  function_results = sort(function_ranges, min_time)
  application_results = sort(application_ranges, min_time)
  return [function_results, application_results]


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--subfolder", required=True, type=str, help="Folder data is in")
  parser.add_argument("--start_range", type=int, help="Start timestamp of zoom region")
  parser.add_argument("--end_range", type=int, help="End timestamp of zoom region")
  args = parser.parse_args()
  results = parse(args.subfolder)
  colors = ["red", "blue"]
  labels = ["Number of VCPUs", "Number of Running Jobs"]
  graph.graph(args.subfolder, results, colors, labels, args.start_range, args.end_range)

main()

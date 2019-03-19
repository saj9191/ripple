import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import sys


def get_pywren_results(file):
  print("****PYWREN****")
  timestamps = json.loads(open(file).read())  
  pywren_cum_cost_x = []
  pywren_cum_cost_lambda = [0]
  jobs = []

  pywren_cum_cost = {}
  pywren_cum_duration = {}
  num_files = 0
  min_duration = sys.maxsize
  max_durations = {}
  duration = 0
  stages = {}
  costs = {}
  ts = timestamps[:3]
  min_timestamp = sys.maxsize
  for job in ts:
    st = sys.maxsize
    et = 0
    for [start, end] in job:
      if start not in stages:
        stages[start] = 0
        costs[start] = 0
        max_durations[start] = 0
      stages[start] += 1
      if end not in pywren_cum_cost:
        pywren_cum_cost[end] = 0
        pywren_cum_duration[end] = 0
      st = min(start, st)
      et = max(end, et)
      pywren_cum_duration[end] += (end-start)
      pywren_cum_cost[end] += (end-start)*100*0.000004897
      costs[start] += (end-start)*100*0.000004897
      max_durations[start] = max(max_durations[start], end-start)
    min_timestamp = min(min_timestamp, st)
    print("WTF", max_durations)
    print("Duration", et - st)
    print("Cost", costs)
  print("Stages", stages)
  print("Max durations", max_durations)

  pywren_cum_cost_x = list(pywren_cum_cost.keys())
  pywren_cum_cost_x.sort()
  cost_y = list(map(lambda k: pywren_cum_cost[k], pywren_cum_cost_x))
  pywren_cum_cost_x = list(map(lambda t: t - min_timestamp, pywren_cum_cost_x))
  pywren_cum_cost_x = [0] + pywren_cum_cost_x
  cost_y = [0] + cost_y

  pywren_cum_cost_lambda = [0]
  for i in range(1, len(cost_y)):
    pywren_cum_cost_lambda.append(pywren_cum_cost_lambda[-1] + cost_y[i])

  print("Cost Lambda", pywren_cum_cost_lambda[-1])
  pywren_cum_cost_ec2 = list(map(lambda x: x * (4.256/3600), pywren_cum_cost_x))

  pywren_cum_cost = []
  assert(len(pywren_cum_cost_lambda) == len(pywren_cum_cost_ec2))
  for i in range(len(pywren_cum_cost_lambda)):
    pywren_cum_cost.append(pywren_cum_cost_lambda[i] + pywren_cum_cost_ec2[i])

  print("****PYWREN****")
  return [pywren_cum_cost_x, pywren_cum_cost_lambda, pywren_cum_cost_ec2, pywren_cum_cost]

def compare(pywren_results, ripple_results):
  [pywren_cum_cost_x, pywren_cum_cost_lambda, pywren_cum_cost_ec2, pywren_cum_cost] = pywren_results
  [ripple_cum_cost_x, ripple_cum_cost] = ripple_results
  plt.plot(pywren_cum_cost_x, pywren_cum_cost_lambda, color="red", label="PyWren Lambda Functions")
  plt.plot(pywren_cum_cost_x, pywren_cum_cost_ec2, color="purple", label="PyWren EC2")
  plt.plot(pywren_cum_cost_x, pywren_cum_cost, color="black", label="Pywren Total")
  plt.plot(ripple_cum_cost_x, ripple_cum_cost, color="blue", label="Ripple Total")
  plt.legend()
  plt.xlabel("Time (Seconds)")
  plt.ylabel("Cumulative Cost ($)")
  plot_name = "pywren_cumulative.png"
  print("Plot", plot_name)
  plt.savefig(plot_name)


def get_ripple_results(folder):
  print("***RIPPLE***")
  params = json.loads(open("../json/spacenet-classification.json").read())
  memory = json.loads(open("../json/memory.json").read())
  min_time = sys.maxsize

  ripple_cum_cost = {}
  num_files = 0
  max_duration = 0
  min_duration = sys.maxsize
  stages = {}
  max_durations = {}
  duration = 0
  for [root, dirs, files] in os.walk(folder):
    if len(dirs) == 0 and num_files < 3:
      st = sys.maxsize
      et = 0 
      files = list(filter(lambda f: not f.endswith(".png") and f != "statistics", files))
      num_files += 1
      for file in files:
        path = root + "/" + file
        stats = json.loads(open(path).read())
        stage = int(file.split(".")[0])
        start_time = stats["start_time"]
        st = min(st, start_time)
        min_time = min(min_time, start_time)
        end_time = start_time + stats["duration"] / 1000.0
        et = max(et, end_time)
        if stage not in stages:
          stages[stage] = [0, 0]
          max_durations[stage] = 0
        max_durations[stage] = max(max_durations[stage], end_time - start_time)
        duration += (stats["duration"] / 1000.0)
        #stages[stage][0] = max(stats["duration"] / 1000.0, stages[stage][0])
        stages[stage][1] += 1
        mkey = params["functions"][stats["name"]]["memory_size"]
        cost = memory["lambda"][str(mkey)]
        ripple_cum_cost[end_time] = (stats["duration"] / 10) * cost
      print("Duration", et - st)

  print("Stages", stages)
  print("Max Duraiton", max_durations)
  ripple_cum_cost_x = list(ripple_cum_cost.keys())
  ripple_cum_cost_x.sort()
  cost_y = list(map(lambda k: ripple_cum_cost[k], ripple_cum_cost_x))
  ripple_cum_cost_x = list(map(lambda t: t - min_time, ripple_cum_cost_x))
  ripple_cum_cost_x = [0] + ripple_cum_cost_x
  cost_y = [0] + cost_y
  ripple_cum_cost = [0]
  for i in range(1, len(cost_y)):
    ripple_cum_cost.append(ripple_cum_cost[-1] + cost_y[i])
  print("Ripple cost", ripple_cum_cost[-1])
  print("***RIPPLE***")
  return [ripple_cum_cost_x, ripple_cum_cost]


def main():
  ripple_results = get_ripple_results("../ec2/lambda_simulations/spacenet-uniform-invoke")
  pywren_results = get_pywren_results("results/spacenet-uniform/pywren")
  compare(pywren_results, ripple_results)


main()

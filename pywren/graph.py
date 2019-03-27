import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import sys


def ec2_results(subfolder):
  stats = json.loads(open(subfolder + "/stats").read())
  start_time = stats["start_time"]
  end_time = stats["end_time"]
  timestamps = stats["timestamps"]
  print("PyWren duration", end_time - start_time)
  return [start_time, end_time, timestamps]


def lambda_results(subfolder, min_time):
  folder = subfolder + "/c2935621-0c18-4b05-9e7c-5d2a6b180f45"
  costs = []
  ranges = []
  for root, _, files in os.walk(folder):
    for f in files:
      if f in ["cpu", "statistics"]:
        continue
      path = root + "/" + f
      try:
        stats = json.loads(open(path, "r").read())
        parts = f.split(".")
        start_time = stats["start_time"]
        end_time = stats["end_time"]
        ranges.append([start_time, 2])
        ranges.append([end_time, -2])
        # 1 second = 1000ms so there are 10 100ms
        costs.append([end_time, ((end_time-start_time) * 10) * 0.000004897])
      except Exception as e:
        pass


  total_cost = sum(list(map(lambda c: c[1], costs)))
  print("Total cost", total_cost)
  cum_costs = [[0, 0]] + cumulative(costs, min_time)
  cum_lambdas = cumulative(ranges, min_time)
  return [cum_costs, cum_lambdas]


def plot(name, results, colors, labels, ylabel):
  marks = [":", "-", "-.", "--"]
  for i in range(len(results)):
    x = list(map(lambda x: x[0], results[i]))
    y = list(map(lambda x: x[1], results[i]))
    plt.plot(x, y, color=colors[i], label=labels[i], linestyle=marks[i])
  plot_name = name + ".png"
  plt.ylabel(ylabel)
#  plt.ylim([0, 6])
  plt.xlabel("Runtime (Seconds)")
  print("Plot", plot_name)
  plt.legend(loc="upper right", frameon=False)
  plt.savefig(plot_name)
  plt.close()


def pywren(name, subfolder):
  [start_time, end_time, timestamps] = ec2_results(subfolder)
  [pywren_costs, pywren_lambda] = lambda_results(subfolder, start_time)
  duration = end_time - start_time

  ec2_costs = []
  for x in list(map(lambda c: c[0], pywren_costs)):
    ec2_costs.append([x, x * (4.256 / 3600)])
  ec2_costs.append([duration, x * (4.256 / 3600)])
  pywren_costs.append([duration, pywren_costs[-1][1]])

  pywren_lambda = list(map(lambda l: [l[0], l[1] + 64], pywren_lambda))
  pywren_lambda = [[0, 64]] + pywren_lambda + [[duration, 64]]

  total_cost = []
  assert(len(pywren_costs) == len(ec2_costs))
  for i in range(len(pywren_costs)):
    total_cost.append([pywren_costs[i][0], pywren_costs[i][1] + ec2_costs[i][1]])
  print("EC2 cost", ec2_costs[-1][1])
  return [pywren_costs, pywren_lambda, ec2_costs, total_cost, duration]


def cumulative(ranges, min_time):
  ranges.sort()
  cum = []
  num = 0
  for [time, r] in ranges:
    num += r
    cum.append([time - min_time, num])
  return cum

def ripple(name, subfolder):
  durations = 0
  count = 0
  min_time = sys.maxsize
  token_to_durations = {}
  costs = []
  ranges = []
  for root, _, files in os.walk(subfolder + "/" +  name):
    for file in files:
      if not file.endswith(".log"):
        continue
      path = root + "/" + file
      parts = file.split(".")
      stage = int(parts[0])
      token = ".".join(parts[1:3])
      if token not in token_to_durations:
        token_to_durations[token] = [sys.maxsize, 0]
      try:
        stats = json.loads(open(path, "r").read())
        start_time = stats["start_time"]
        min_time = min(start_time, min_time)
        end_time = start_time + stats["duration"] / 1000.0
        token_to_durations[token][0] = min(token_to_durations[token][0], start_time)
        token_to_durations[token][1] = max(token_to_durations[token][1], end_time)
        # Duration in milliseconds
        # So if the duration is 100ms, then we are changed 1 unit so 100ms / 100 * cost
        costs.append([end_time, (stats["duration"] / 100) * 0.000004897])
        ranges.append([start_time, 2])
        ranges.append([end_time, -2])
      except Exception as e:
        pass

  total_cost = sum(list(map(lambda c: c[1], costs)))
  print("Total cost", total_cost)
  print("Num runs", len(token_to_durations))
  for token in token_to_durations:
    print("Duration", token_to_durations[token][1] - token_to_durations[token][0])
  cum_costs = cumulative(costs, min_time)
  cum_lambdas = cumulative(ranges, min_time)
  return [cum_costs, cum_lambdas]


def main():
  subfolder = "results/single-spacenet"
  colors = ['#003300', '#883300','#ff3300', '#000000']
  print("Ripple")
  [ripple_cost, ripple_lambdas] = ripple("ripple", subfolder)
  print("PyWren")
  [pywren_cost, pywren_lambdas, ec2_cost, total_cost, duration] = pywren("pywren", subfolder)
  ripple_cost.append([duration, ripple_cost[-1][1]])
  #ripple_lambdas.append([duration, 0])
  plot("pywren-cost", [ripple_cost, pywren_cost, ec2_cost, total_cost], colors, ["Ripple", "PyWren (Lambda)", "PyWren (EC2)", "PyWren (Total)"], "Cumulative Cost ($)")
  plot("pywren-lambdas", [ripple_lambdas, pywren_lambdas], colors, ["Ripple", "PyWren"], "VCPUs")
#  subfolder = "results/compression"
#  print("Low")
#  ripple("ripple-low", subfolder)
#  print("Max")
#  ripple("ripple-high", subfolder)
#  print("PyWren")
#  pywren("pywren", subfolder)

main()

import argparse
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os


def parse_pywren(folder):
  cpus = []
  max_cpu = (3008 * 200) / 3008
  for root, dirs, files in os.walk(folder):
    for file in files:
      with open(folder + "/" + file, "r") as f:
        content = f.read().strip()
        if len(content) > 0:
          cpus += json.loads(content)["cpu"]

  print("MAX", max(cpus))
  cpus = list(map(lambda cpu: cpu / max_cpu, cpus))
  print("WTF", max(cpus))
  return cpus


def parse_ripple(folder, parameters):
  cpus = []
  for root, dirs, _ in os.walk(folder):
    for dir in dirs:
      for _, _, files in os.walk(folder + "/" + dir):
        for file in files:
          with open(folder + "/" + dir + "/" + file, "r") as f:
            content = f.read().strip()
            if len(content) > 0:
              stats = json.loads(content)
              if "cpu" in stats:
                stage = int(file.split(".")[0])
                max_cpu = (parameters["functions"][parameters["pipeline"][stage]["name"]]["memory_size"] * 200) / 3008
                cpus += list(map(lambda cpu: cpu / max_cpu, stats["cpu"]))
  return cpus


def cdf(cpus):
  cpu_to_counts = {}
  for cpu in cpus:
    if cpu not in cpu_to_counts:
      cpu_to_counts[cpu] = 0
    cpu_to_counts[cpu] += 1

  X = np.array(list(cpu_to_counts.keys()))
  X.sort()
  Y = list(map(lambda cpu: cpu_to_counts[cpu], X))
  s = sum(Y)
  Y = list(map(lambda cpu: cpu / s, Y))
  CY = np.cumsum(Y) 
  return [X, CY]


def plot(subfolder, results, labels):
  fig, ax = plt.subplots()
  for i in range(len(results)):
    [x, cy] = results[i]
    plt.plot(x, cy, label=labels[i])
  plot_name = subfolder + "/simulation.png"
  print("Plot", plot_name)
  ax.legend()
  fig.savefig(plot_name)
  plt.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--pywren", type=str, required=True)
  parser.add_argument("--ripple", type=str, required=True)
  parser.add_argument("--parameters", type=str, required=True)
  args = parser.parse_args()
  pywren_cpus = parse_pywren(args.pywren) 
  ripple_cpus = parse_ripple(args.ripple, json.loads(open(args.parameters).read()))
  [px, pcy] = cdf(pywren_cpus)
  [rx, rcy] = cdf(ripple_cpus)
  labels = ["PyWren", "Ripple"]
  plot(args.ripple, [[px, pcy], [rx, rcy]], labels)


main()

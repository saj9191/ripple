import matplotlib
import os
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def update_score(specie_scores, specie, score, index):
  if index in specie_scores:
    [cur_specie, cur_score] = specie_scores[index]
    if cur_score < score:
      specie_scores[index] = [specie, score]
  else:
    specie_scores[index] = [specie, score]


def calculate_score(file, specie, threshold):
  specie_scores = {}
  f = open(file)
  lines = f.readlines()
  cost = sum(list(map(lambda c: float(c), lines[1].split(",")[1:])))
  lines = lines[2:]
  file_count = 0
  for line in lines:
    parts = line.strip().split(",")
    file_count = len(parts)
    fasta = parts[0]
    file_scores = parts[1:]
    for index in range(len(file_scores)):
      if len(file_scores[index]) == 0:
        score = 0
      else:
        score = int(file_scores[index])
      update_score(specie_scores, fasta, score, index)

  total_count = len(specie_scores)
  correct_count = 0
  unknown_count = 0
  for index in specie_scores:
    [top_specie, top_count] = specie_scores[index]
    if top_count < threshold:
      unknown_count += 1
    elif top_specie == specie:
      correct_count += 1
#    else:
#      print("top", top_specie, "expected", specie, file)

  return [correct_count, unknown_count, total_count, file_count, cost]


def calculate_scores(threshold):
  scores = {}
  tops = [
    500,
    1000,
    2000,
  ]

  datasets = {
    "ALS_CSF_Biomarker_Study-1530309740015": "normalHuman",
    "Coon-SingleShotFusionYeast": "normalYeast",
    "Coon-HeLa-APD-Data": "normalHuman",
    "Differential_analysis_of_a_synaptic_density_fraction_from_Homer2_knockout_and_wild-type_olfactory_bulb_digests-1534804159922": "normalMouse",
    "Differential_analysis_of_hemolyzed_mouse_plasma-1534802693575": "normalMouse",
    "Momo_Control_Yeast_DDA": "normalYeast",
    "PES_Unfractionated-1533935400961": "normalRoundworm",
    "PXD001873": "silacLys6Arg6Human",
    "PXD002079": "phosphorylationHuman",
    "PXD005323": "normalHuman",
    "PXD005709": "normalHuman",
    "PXD009227": "phosphorylationHuman",
    "PXD001250-Mann_Mouse_Brain_Proteome": "normalMouse",
    "PXD002801-TMT10": "tmt6Mouse",
    "PXD003177": "itraq8Mouse",
    "PXD009220": "itraq8Mouse",
    "PXD009240": "phosphorylationFruitFly"
  }

  folders = list(datasets.keys())
  costs = { "medic_": 0.0, "": 0.0 }
  count = 0
  for top in tops:
    for medic in ["medic_", ""]:
      token = "{0:s}{1:d}".format(medic, top)
      if token not in scores:
        scores[token] = [0, 0, 0]
      for folder in folders:
        if os.path.isdir("data_counts/{0:s}/{1:s}".format(token, folder)):
          for root, dirs, files in os.walk("data_counts/{0:s}".format(token)):
            if len(files) > 0:
              for file in files:
                csv_file = "{0:s}/{1:s}".format(root, file)
                counts = calculate_score(csv_file, datasets[folder], threshold * top)
                count += counts[-2]
                costs[medic] += counts[-1]
                for i in range(len(counts[:3])):
                  scores[token][i] += counts[i]
        else:
          print("Cannot find top", token, "for", folder)
          pass

  correct = {}
  wrong = {}
  unknown = {}
  for top in tops:
    for medic in ["medic_", ""]:
      token = "{0:s}{1:d}".format(medic, top)
      if token in scores:
        [correct_count, unknown_count, total_count] = scores[token]
        wrong_count = total_count - correct_count - unknown_count
        correct[token] = (float(correct_count) / total_count) * 100
        unknown[token] = (float(unknown_count) / total_count) * 100
        wrong[token] = (float(wrong_count) / total_count) * 100
        print("Top", token, "Correct count", correct_count)
        print("Top", token, "Wrong count", wrong_count)
        print("Top", token, "Unknown count", unknown_count)
        print("Top", token, "Total count", total_count)
        print("")

  print("COUNT", count)
  print("PARAMEDIC COST", costs["medic_"] / count)
  print("NORMAL COST", costs[""] / count)
  return [correct, unknown, wrong]


def graph(correct, unknown, wrong, threshold):
  fig, ax = plt.subplots()
  #x_values = list(correct.keys())
  #x_values.sort()
  print(correct)
  print(unknown)
  print(wrong)
  x_values = ["medic_500", "medic_1000", "medic_2000", "500", "1000", "2000"]
  indices = range(len(x_values))
  correct_values = []
  wrong_values = []
  unknown_values = []
  wrong_bottom = []

  for x_value in x_values:
    correct_values.append(correct[x_value])
    unknown_values.append(unknown[x_value])
    wrong_bottom.append(correct[x_value] + unknown[x_value])
    wrong_values.append(wrong[x_value])

  x_values = ["m500", "m1000", "m2000", "500", "1000", "2000"]
  p0 = plt.bar(indices, correct_values)
  p1 = plt.bar(indices, unknown_values, bottom=correct_values)
  p2 = plt.bar(indices, wrong_values, bottom=wrong_bottom)
  plt.legend((p0[0], p1[0], p2[0]), ("Correct", "Unknown", "Wrong"))
  plt.ylabel("Percentage (%)")
  plt.title("Spectra Labels (Threshold={0:f}%)".format(threshold * 100))
  plt.xticks(indices, x_values)
  plot_name = "data_counts/accuracy_{0:f}.png".format(threshold)
  print("Plot", plot_name)
  fig.savefig(plot_name)
  plt.close()


def graph_thresholds():
  fig, ax = plt.subplots()
  thresholds = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7]
  threshold_results = {}

  for threshold in thresholds:
    threshold_results[threshold] = calculate_scores(threshold)

  for threshold in thresholds:
    print(threshold, threshold_results[threshold])
    print("")

  datasets = ["medic_500", "medic_1000", "medic_2000", "500", "1000", "2000"]
  for dataset in datasets:
    x_values = []
    y_values = []
    for threshold in thresholds:
      [correct, unknown, wrong] = threshold_results[threshold]
      x_values.append(correct[dataset])
      y_values.append(wrong[dataset])
    plt.plot(x_values, y_values, marker="x", label=dataset)

  plt.ylabel("Wrong Percentage")
  plt.xlabel("Correct Percentage")
  plt.legend()
  plot_name = "data_counts/thresholds.png"
  print("Plot", plot_name)
  fig.savefig(plot_name)
  plt.close()


def main():
  #graph_thresholds()

  threshold = 0.25
  [correct, unknown, wrong] = calculate_scores(threshold)
  graph(correct, unknown, wrong, threshold)


if __name__ == "__main__":
  main()

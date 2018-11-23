import shutil
import subprocess


def get_score(file_name):
  lines = open(file_name).readlines()[1:]
  values = list(map(lambda line: float(line.split("\t")[9]), lines))
  return len(list(filter(lambda v: v <= 0.01, values)))


def get_confidence(file_name, specie):
  subprocess.call("./crux tide-search {0:s} {1:s}.index --concat T --txt-output T".format(file_name, specie), shell=True)
  subprocess.call("./crux assign-confidence crux-output/tide-search.txt", shell=True)
  score = get_score("crux-output/assign-confidence.target.txt")
  shutil.rmtree("crux-output")
  return score


def calculate_confidences(file_name):
  specie_to_score = {}
  for specie in ["Boar", "Human", "Mouse", "Rat", "RedJunglefowl", "Zebrafish"]:
    score = get_confidence(file_name, "normal" + specie)
    specie_to_score[specie] = score

  for specie in specie_to_score:
    print(specie, specie_to_score[specie])


calculate_confidences("HOMER2_KO_A.mzML")

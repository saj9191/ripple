import argparse
import boto3
import clear
import inspect
import math
import matplotlib
import os
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
ET.register_namespace("", "http://psi.hupo.org/ms/mzml")
matplotlib.use('Agg')
import matplotlib.pyplot as plt

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
import upload


def cv_param(spectrum, name):
  for cv_param in spectrum.iter("{http://psi.hupo.org/ms/mzml}cvParam"):
    if cv_param.get("name") == name:
      return float(cv_param.get("value"))


def scan_id(spectrum):
  id_field = spectrum.get("id")
  return int(id_field.split(" ")[2].split("=")[1])


def spectra_intensities(file_name):
  spectra = ET.parse(file_name).getroot()[0][6][0]
  spectra = list(filter(lambda spectrum: cv_param(spectrum, "ms level") == 2.0, spectra))
  spectra = list(map(lambda spectrum: [scan_id(spectrum), cv_param(spectrum, "total ion current")], spectra))
  spectra = sorted(spectra, key=lambda spectrum: -spectrum[1])
  return spectra


def setup_files(folder, file_name, fasta):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("tide-source-data")

  output_dir = "crux-output-{0:s}".format(fasta)
  if os.path.isdir(output_dir):
    shutil.rmtree(output_dir)

  if not os.path.isfile(file_name):
    with open(file_name, "wb") as f:
      bucket.download_fileobj(folder + "/" + file_name, f)


def scores(file_name, scan_index, score_index):
  f = open(file_name)
  lines = f.readlines()
  parts = lines[0].split("\t")
  field = parts[score_index]
  assert(field == "percolator q-value" or "tdc q-value")
  field = parts[scan_index]
  assert(field == "scan")
  lines = lines[1:]
  scores = {}
  for line in lines:
    parts = line.split("\t")
    scan_id = int(parts[scan_index])
    score = float(parts[score_index])
    scores[scan_id] = [score, parts[10]]
  return scores


def bin_spectra(spectra, percolator_scores, confidence_scores):
  percolator_bins = {}
  confidence_bins = {}

  def increment(bins, bin_id, scores, scan_id):
    if bin_id not in bins:
      bins[bin_id] = [0, 0]

    bins[bin_id][1] += 1

    if scan_id in scores and scores[scan_id][0] <= 0.01:
      bins[bin_id][0] += 1

#  max_intensity = spectra[0][1]
  for i in range(len(spectra)):
    [scan_id, intensity] = spectra[i]
    bin_id = math.exp(int(math.log(intensity)))
    increment(percolator_bins, bin_id, percolator_scores, scan_id)
    increment(confidence_bins, bin_id, confidence_scores, scan_id)

  assert(len(percolator_bins) == len(confidence_bins))
  return [percolator_bins, confidence_bins]


def graph(file_name, spectra, percolator_bins, percolator_percentages):
  bin_y = []
  total_y = []
  percentage_y = []
  fig, ax1 = plt.subplots()
  bins = list(percolator_bins.keys())
  bins.sort()
  for i in bins:
    bin_y.append(percolator_bins[i][0])
    total_y.append(percolator_bins[i][1])
  ax1.plot(bins, bin_y, "r--")
  ax1.plot(bins, total_y, "g-")
  ax1.set_xscale("log")

  ax2 = ax1.twinx()
  percentages = list(percolator_percentages.keys())
  percentages.sort()
  for i in bins:
    percentage_y.append(percolator_percentages[i])

  ax2.plot(percentages, percentage_y, "bs")
  ax2.set_xscale("log")
  fig.legend(["Match Count", "Total Count", "Percentage"], loc="upper left")

  for v in [500, 1000]:
    plt.axvline(x=spectra[v][1])

  plt.title(file_name)
  fig.savefig("results.png")
  plt.close()


def generate(bucket, file_name, fasta):
  setup_files(bucket, file_name, fasta)
  output_dir = "crux-output-{0:s}".format(fasta)
  subprocess.call("./crux tide-search {0:s} {1:s}.index --concat T --txt-output T --output-dir {2:s}".format(file_name, fasta, output_dir), shell=True)
  subprocess.call("./crux percolator {0:s}/tide-search.txt --output-dir {0:s}".format(output_dir), shell=True)
  subprocess.call("./crux assign-confidence {0:s}/tide-search.txt --output-dir {0:s}".format(output_dir), shell=True)


def confident_scan_ids(scores):
  scan_ids = list(filter(lambda scan_id: scores[scan_id][0] <= 0.01, list(scores.keys())))
  return scan_ids


def print_peptides(peptides):
  for peptide in peptides:
    print(peptide)
  print("")

def print_sets():
  folder = "ALS_CSF_Biomarker_Study"
  file_name = "TN_CSF_062617_03.mzML"
  generate(folder, file_name, "normalHuman")
  generate(folder, file_name, "phosphorylationHuman")
  normal_human_scores = scores("crux-output-normalHuman/percolator.target.psms.txt", 1, 7)
  phosphorylation_human_scores = scores("crux-output-phosphorylationHuman/percolator.target.psms.txt", 1, 7)

  # normal_set = set(confident_scan_ids(normal_human_scores))
  # phosphorylation_set = set(confident_scan_ids(phosphorylation_human_scores))

  normal_peptide_set = set(list(map(lambda score: score[1], normal_human_scores.values())))
  phosphorylation_peptide_set = set(list(map(lambda score: score[1], phosphorylation_human_scores.values())))

  print("INTERSECTION")
  intersection = normal_peptide_set.intersection(phosphorylation_peptide_set)
  print_peptides(intersection)

  print("NORMAL HUMAN")
  normal = normal_peptide_set.difference(phosphorylation_peptide_set)
  print_peptides(normal)

  print("PHOSPHORYLATION HUMAN")
  phosphorylation = phosphorylation_peptide_set.difference(normal_peptide_set)
  print_peptides(phosphorylation)


def calculate_percentages(bins):
  percentages = {}
  total = 0
  for bin_id in bins:
    total += bins[bin_id][1]
    percentages[bin_id] = (float(bins[bin_id][0]) / bins[bin_id][1]) * 100
  print("Total", total)
  return percentages


def graph_intensities(folder, file_name, fasta):
  generate(folder, file_name, fasta)
  spectra = spectra_intensities(file_name)
  percolator_scores = scores("crux-output-{0:s}/percolator.target.psms.txt".format(fasta), 1, 7)
  confidence_scores = scores("crux-output-{0:s}/assign-confidence.target.txt".format(fasta), 1, 9)
  [percolator_bins, confidence_bins] = bin_spectra(spectra, percolator_scores, confidence_scores)
  percolator_percentages = calculate_percentages(percolator_bins)
#  confidence_percentages = calculate_percentages(confidence_bins)
#  print("percolator", percolator_percentages)
#  print("")
#  print("confidence", confidence_percentages)
#  print("")
#  graph(percolator_bins, confidence_bins)
  graph(file_name, spectra, percolator_bins, percolator_percentages)



def setup(folder, top):
  confidence_folder = "confidence_scores"
  if not os.path.isdir(confidence_folder):
    os.mkdir(confidence_folder)

  top_folder = confidence_folder + "/" + str(top)
  if not os.path.isdir(top_folder):
    os.mkdir(top_folder)

  subset_folder = top_folder + "/" + folder
  if not os.path.isdir(subset_folder):
    os.mkdir(subset_folder)

  return subset_folder


def process_files(folder, subset_folder):
  s3 = boto3.resource("s3")
  source_bucket = s3.Bucket("tide-source-data")
  data_bucket = s3.Bucket("maccoss-tide")
  objects = list(source_bucket.objects.filter(Prefix=folder))
  for obj in objects:
    obj_folder = subset_folder + "/" + obj.key.split("/")[-1]
    if not os.path.isdir(obj_folder):
      os.mkdir(obj_folder)

    upload.upload("maccoss-tide", obj.key, "tide-source-data")
    keys = []

    while len(keys) < 1:
      keys = list(map(lambda o: o.key, list(data_bucket.objects.filter(Prefix="5/"))))
      time.sleep(10)

    match_file = obj_folder + "/match"
    with open(match_file, "wb+") as f:
      data_bucket.download_fileobj(keys[0], f)

    top_match = open(match_file).read().strip()
    with open(obj_folder + "/confidence", "wb+") as f:
      data_bucket.download_fileobj(top_match, f)
    clear.clear("maccoss-tide")
    clear.clear("maccoss-log")


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, required=True, help="Name of folder file resides in")
  #parser.add_argument("--file", type=str, required=True, help="Name of file to analyze")
  #parser.add_argument("--fasta", type=str, required=True, help="FASTA index to use")
  parser.add_argument("--top", type=int, required=True, help="Number of top spectra to use")
  args = parser.parse_args()

  subset_folder = setup(args.folder, args.top)
  process_files(args.folder, subset_folder)

#  graph_intensities(args.folder, args.file, args.fasta)
#  os.remove(args.file)


if __name__ == "__main__":
  main()

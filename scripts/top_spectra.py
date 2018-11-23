import boto3
import matplotlib
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
ET.register_namespace("", "http://psi.hupo.org/ms/mzml")
matplotlib.use('Agg')
import matplotlib.pyplot as plt


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


def bin_spectra(spectra, percolator_scores, confidence_scores, bin_size):
  percolator_bins = {}
  confidence_bins = {}

  def increment(bins, bin_id, scores, scan_id):
    if bin_id not in bins:
      bins[bin_id] = 0
    if scan_id in scores and scores[scan_id][0] <= 0.01:
      bins[bin_id] += 1

  max_intensity = spectra[0][1]
  # print("max intensity", max_intensity)
  for i in range(len(spectra)):
    [scan_id, intensity] = spectra[i]
    bin_id = int((float(intensity) / max_intensity) * 100)
    # print("id", scan_id, "intensity", intensity, "max", max_intensity, "bin", bin_id)
    # if scan_id not in percolator_scores:
    #   print("percolator confidence", "N/A")
    # else:
    #   print("percolator confidence", percolator_scores[scan_id][0])
    # if scan_id not in confidence_scores:
    #   print("confidence confidence", "N/A")
    # else:
    #   print("confidence confidence", confidence_scores[scan_id][0])
    increment(percolator_bins, bin_id, percolator_scores, scan_id)
    increment(confidence_bins, bin_id, confidence_scores, scan_id)

  assert(len(percolator_bins) == len(confidence_bins))
  return [percolator_bins, confidence_bins]


def graph(percolator_bins, confidence_bins):
  py = []
  cy = []
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  bins = list(percolator_bins.keys())
  bins.sort()
#  num_bins = len(percolator_bins)
  for i in bins: #range(num_bins - 1, -1, -1):
    py.append(percolator_bins[i])
    if i in confidence_bins:
      cy.append(confidence_bins[i])
    else:
      cy.append(0)

  print("bins", bins)
  print("py", py)
  print("cy", cy)
  ax.plot(bins, py, "r--", bins, cy, "bs")
  ax.set_yscale("log")
  fig.legend(["Percolator", "Confidence"], loc="upper left")
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


def graph_intensities():
  file_name = "TN_CSF_062617_03.mzML"
  prefix = "normalHuman"
  bucket = "ALS_CSF_Biomarker_Study-1530309740015"
  spectra = spectra_intensities(file_name)
  print(spectra[:100])
#  generate(bucket, file_name, prefix)
  percolator_scores = scores("crux-output-{0:s}/percolator.target.psms.txt".format(prefix), 1, 7)
  confidence_scores = scores("crux-output-{0:s}/assign-confidence.target.txt".format(prefix), 1, 9)
  [percolator_bins, confidence_bins] = bin_spectra(spectra, percolator_scores, confidence_scores, 1000)
  graph(percolator_bins, confidence_bins)


graph_intensities()

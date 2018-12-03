import argparse
import boto3
import inspect
import json
import os
import print_species
import sys
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import clear
import setup
import statistics
import upload
import util


species = [
  "Arabidopsis",
  "Boar",
  "Cattle",
  "Ecoli",
  "FruitFly",
  "Human",
  "Mouse",
  "Rat",
  "RedJunglefowl",
  "Rice",
  "Roundworm",
  "Trypanosoma",
  "Tuberculosis",
  "Yeast",
  "Zebrafish"
]


def run(folder, top):
  os.chdir("..")
  params = json.loads(open("json/private-tide.json").read())
  params["sample_bucket"] = "tide-source-data"
  params["folder"] = "tide"
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key

  #setup.setup(params)

  s3 = boto3.resource("s3")
  sample_bucket_name = "tide-source-data"
  sample_bucket = s3.Bucket(sample_bucket_name)
  objects = list(sample_bucket.objects.filter(Prefix=folder))

  data_folder = "scripts/data_counts"
  if not os.path.isdir(data_folder):
    os.mkdir(data_folder)

  top_folder = data_folder + "/" + str(top)
  if not os.path.isdir(top_folder):
    os.mkdir(top_folder)

  file_to_results = {}
  file_to_costs = {}

  for obj in objects:
    key = obj.key
    s3_key, _, _ = upload.upload(params["bucket"], key, sample_bucket_name)
    token = s3_key.split("/")[1]
    params["key"] = s3_key
    species_to_score = print_species.run(token)
    file_to_results[key] = species_to_score
    [costs, _] = statistics.statistics(params["log"], token=token, prefix=None, params=params, show=False)
    file_to_costs[key] = costs
    clear.clear(params["bucket"], token, None)
    clear.clear(params["log"], token, None)

  keys = list(file_to_results.keys())
  keys.sort()

  bucket_file = top_folder + "/" + folder + ".csv"

  f = open(bucket_file, "w+")
  # Header line
  for key in keys:
    f.write(",{0:s}".format(key))
  f.write("\n")

  # Cost line
  f.write("Cost")
  for key in keys:
    f.write(",{0:f}".format(file_to_costs[key][-1]))
  f.write("\n")

  fastas = list(file_to_results[keys[0]].keys())
  fastas.sort()

  sorted_fastas = []
  for specie in species:
    for fasta in fastas:
      if fasta.endswith(specie):
        sorted_fastas.append(fasta)

  # Fasta scores
  for fasta in sorted_fastas:
    f.write(fasta)
    for key in keys:
      if key not in file_to_results or fasta not in file_to_results[key]:
        v = 0
      else:
        v = file_to_results[key][fasta]
      f.write(",{0:d}".format(v))
    f.write("\n")

  f.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, required=True, help="Name of folder file resides in")
  parser.add_argument("--top", type=int, required=True, help="Number of top spectra to use")
  args = parser.parse_args()
  run(args.folder, args.top)


if __name__ == "__main__":
  main()

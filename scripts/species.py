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


def run(folder, top, prefix, pfile, typ):
  os.chdir("..")
  params = json.loads(open(pfile).read())
#  params["sample_bucket"] = "tide-source-data"
  params["folder"] = "tide"
  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key

  #setup.setup(params)

  s3 = boto3.resource("s3")
  sample_bucket_name = params["sample_bucket"]#"tide-source-data"
  sample_bucket = s3.Bucket(sample_bucket_name)

  p = folder
  if prefix is not None:
    p += "/" + prefix
  objects = list(sample_bucket.objects.filter(Prefix=p))
  objects = list(filter(lambda obj: obj.key.endswith(".mzML"), objects))
  objects.reverse()

  data_folder = "scripts/data_counts"
  if not os.path.isdir(data_folder):
    os.mkdir(data_folder)

  if typ == "medic":
    top_folder = data_folder + "/medic_" + str(top)
  elif typ == "ppm":
    top_folder = data_folder + "/ppm_" + str(top)
  else:
    top_folder = data_folder + "/" + str(top)
  if not os.path.isdir(top_folder):
    os.mkdir(top_folder)

  sorted_fastas = None

  prefix = "4/" if typ == "normal" else "5/"
  for obj in objects:
    key = obj.key
    if "/" in key:
      parts = key.split("/")
      folder = top_folder + "/" + "/".join(parts[:-1])
      file_key = parts[-1]

      if not os.path.isdir(folder):
        os.mkdir(folder)
    else:
      file_key = key
      folder = top_folder

    path = folder + "/" + file_key
    if os.path.isfile(path):
      continue
    open(path, "a").close()

    s3_key, _, _ = upload.upload(params["bucket"], key, sample_bucket_name)
    token = s3_key.split("/")[1]
    params["key"] = s3_key
    species_to_score = print_species.run(params["bucket"], prefix, token)
    fastas = list(species_to_score.keys())
    fastas.sort()
    sorted_fastas = []
    for specie in species:
      for fasta in fastas:
        if fasta.endswith(specie):
          sorted_fastas.append(fasta)

    [_, costs] = statistics.statistics(params["log"], token, None, params, None)

    print(path)
    with open(path, "w+") as f:
      f.write("Cost,{0:f}\n".format(costs[-1]))
      # Fasta scores
      for fasta in sorted_fastas:
        f.write(fasta)
        if fasta in species_to_score:
          v = species_to_score[fasta]
        else:
          v = 0
        f.write(",{0:d}\n".format(v))

    clear.clear(params["bucket"], token, None)
    clear.clear(params["log"], token, None)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, required=True, help="Name of folder file resides in")
  parser.add_argument("--top", type=int, required=True, help="Number of top spectra to use")
  parser.add_argument("--parameters", type=str, required=True, help="Parameter file to use")
  parser.add_argument("--prefix", type=str, default=None, help="Object prefix to filter on")
  args = parser.parse_args()

  typ = "normal"
  if "medic" in args.parameters:
    typ = "medic"
  elif "ppm" in args.parameters:
    typ = "ppm"
  print("Type", typ)
  run(args.folder, args.top, args.prefix, args.parameters, typ)


if __name__ == "__main__":
  main()

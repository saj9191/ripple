import boto3
import clear
import json
import print_species
import setup
import statistics
import upload
import util

params = json.loads(open("json/private-tide.json").read())
params["sample_bucket"] = "tide-source-data"
params["folder"] = "tide"
[access_key, secret_key] = util.get_credentials(params["credential_profile"])
params["access_key"] = access_key
params["secret_key"] = secret_key

setup.setup(params)

s3 = boto3.resource("s3")
sample_bucket_name = "tide-source-data"
sample_bucket = s3.Bucket(sample_bucket_name)
objects = list(sample_bucket.objects.filter(Prefix="Differential_analysis_of_a_synaptic_density_fraction_from_Homer2_knockout_and_wild-type_olfactory_bulb_digests-1534804159922"))

file_to_results = {}
file_to_costs = {}

for obj in objects:
  key = obj.key
  s3_key, _, _ = upload.upload(params["bucket"], key, sample_bucket_name)
  token = s3_key.split("/")[1]
  params["key"] = s3_key
  species_to_score = print_species.run()
  file_to_results[key] = species_to_score
  [costs, _] = statistics.statistics(params["log"], token=token, prefix=None, params=params, show=False)
  file_to_costs[key] = costs
  clear.clear(params["bucket"], None, None)
  clear.clear(params["log"], None, None)

keys = list(file_to_results.keys())
keys.sort()

f = open("results.csv", "w+")
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

sorted_fastas = []
for specie in species:
  for fasta in fastas:
    if fasta.endswith(specie):
      sorted_fastas.append(fasta)

# Fasta scores
for fasta in sorted_fastas:
  f.write(fasta)
  for key in keys:
    f.write(",{0:d}".format(file_to_results[key][fasta]))
  f.write("\n")

f.close()

import boto3
import os
import subprocess
import util


def run(file, params, input_format, output_format):
  util.print_read(input_format, file, params)

  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket(params["database_bucket"])

  if "species" in params:
    species = params["species"]
  elif "target_file" in params["extra_params"]:
    species = params["extra_params"]["target_file"]

  with open("/tmp/fasta", "wb") as f:
    database_bucket.download_fileobj("{0:s}/fasta".format(species), f)

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  index_files = ["auxlocs", "pepix", "protix"]
  if not os.path.isdir("/tmp/fasta.index"):
    os.mkdir("/tmp/fasta.index")

  for index_file in index_files:
    name = "{0:s}/{1:s}".format(species, index_file)
    with open("/tmp/fasta.index/{0:s}".format(index_file), "wb") as f:
      database_bucket.download_fileobj(name, f)

  output_dir = "/tmp/crux-output-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    "--num-threads", str(params["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
    "--overwrite", "T"
  ]

  command = "cd /tmp; ./crux tide-search {0:s} fasta.index {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  input_file = "{0:s}/tide-search.txt".format(output_dir)
  output_format["suffix"] = species
  output_format["ext"] = "txt"
  output_file = "{0:s}/{1:s}".format(output_dir, util.file_name(output_format))
  os.rename(input_file, output_file)
  return [output_file]

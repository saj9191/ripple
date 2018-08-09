import boto3
import os
import subprocess
import util


def run(file, params, m):
  util.print_request(m, params)
  util.print_read(m, file, params)

  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  output_dir = "/tmp/percolator-crux-output-{0:f}-{1:d}".format(m["timestamp"], m["nonce"])

  arguments = [
    "--subset-max-train", str(params["max_train"]),
    "--quick-validation", "T",
    "--output-dir", output_dir,
  ]

  command = "cd /tmp; ./crux percolator {0:s} {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  output_files = []
  m["file_id"] = 1
  m["bin"] = 1
  m["more"] = False
  for item in ["target.psms", "decoy.psms", "target.peptides", "decoy.peptides"]:
    input_file = "{0:s}/percolator.{1:s}.txt".format(output_dir, item)
    prefix = "percolator.{0:s}".format(item)
    m["prefix"] = prefix
    output_file = "{0:s}/{1:s}".format(output_dir, util.file_name(m))
    os.rename(input_file, output_file)
    output_files.append(output_file)

  return output_files

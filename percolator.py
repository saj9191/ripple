import boto3
import subprocess


def run(file, params, m):
  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  output_dir = "/tmp/percolator-crux-output-{0:f}-{1:d}".format(m["timestamp"], m["nonce"])

  arguments = [
    "--subset-max-train", str(params["max_train"]),
    "--quick-validation", "T",
    "--output-dir", output_dir
  ]

  command = "cd /tmp; ./crux percolator {0:s} {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  output_files = []
  for item in ["target.psms", "decoy.psms", "target.peptides", "decoy.peptides"]:
    output_files.append("{0:s}/percolator.{1:s}.txt".format(output_dir, item))
  return output_files

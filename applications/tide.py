import boto3
import os
import subprocess
import util


def run(file, params, m):
  util.print_request(m, params)
  util.print_read(m, file, params)

  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")

  with open("/tmp/HUMAN.fasta.20170123", "wb") as f:
    database_bucket.download_fileobj("HUMAN.fasta.20170123", f)

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  index_files = ["auxlocs", "pepix", "protix"]
  if not os.path.isdir("/tmp/HUMAN.fasta.20170123.index"):
    os.mkdir("/tmp/HUMAN.fasta.20170123.index")

  for index_file in index_files:
    with open("/tmp/HUMAN.fasta.20170123.index/{0:s}".format(index_file), "wb") as f:
      database_bucket.download_fileobj(index_file, f)

  output_dir = "/tmp/crux-output-{0:f}-{1:d}".format(m["timestamp"], m["nonce"])

  arguments = [
    "--num-threads", str(params["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
    "--overwrite", "T"
  ]

  command = "cd /tmp; ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  input_file = "{0:s}/tide-search.txt".format(output_dir)
  p = dict(m)
  p["prefix"] = "tide"
  p["ext"] = "txt"
  output_file = "{0:s}/{1:s}".format(output_dir, util.file_name(p))
  with open(input_file) as inf:
    with open(output_file, "w+") as outf:
      content = inf.read()
      index = content.find("\n")
      outf.write(content[index + 1:])

  return [output_file]

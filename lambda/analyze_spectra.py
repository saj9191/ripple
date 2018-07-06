import boto3
import json
import os
import subprocess
import time
import util

INPUT_FILE = util.spectra_regex("ms2")


def analyze_spectra(bucket_name, spectra_file, num_threads):
  util.clear_tmp()
  m = INPUT_FILE.match(spectra_file)
  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")
  spectra_bucket = s3.Bucket(bucket_name)
  output_bucket = s3.Bucket("maccoss-human-output-spectra")

  with open("/tmp/{0:s}".format(spectra_file), "wb") as f:
    spectra_bucket.download_fileobj(spectra_file, f)

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

  ts = m.group(1)
  output_dir = "/tmp/crux-output-{0:s}".format(ts)
  print(spectra_file, subprocess.check_output("cat /tmp/{0:s} | grep MS1Intensity | wc -l".format(spectra_file), shell=True))

  arguments = [
    "--num-threads", str(num_threads),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
  ]

  command = "cd /tmp; ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(spectra_file, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  done = False
  while not done:
    process_output = str(subprocess.check_output("ps aux | grep crux", shell=True))
    done = len(process_output.split("\n")) == 1
    time.sleep(1)

  output_file = "{0:s}/tide-search.txt".format(output_dir)
  if os.path.isfile(output_file):
    print(output_file, subprocess.check_output('cat {0:s} | grep "<spectrum_q" | wc -l'.format(output_file), shell=True))
    output = open(output_file).read()
    output_bucket.put_object(Key=spectra_file.replace(".ms2", ".txt"), Body=str.encode(output))
  else:
    print("ERROR", output_file, "does not exist")


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  spectra_file = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("analyze_spectra.json").read())
  analyze_spectra(bucket_name, spectra_file, params["num_threads"])

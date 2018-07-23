import boto3
import json
import subprocess
import time
import util


def run_percolator(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  print("TIMESTAMP {0:f}".format(ts))

  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")
  spectra_bucket = s3.Bucket(bucket_name)

  with open("/tmp/{0:s}".format(key), "wb") as f:
    spectra_bucket.download_fileobj(key, f)

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)

  output_dir = "/tmp/percolator-crux-output-{0:f}".format(ts)

  arguments = [
    "--subset-max-train", str(params["max_train"]),
    "--quick-validation", "T",
    "--output-dir", output_dir
  ]

  command = "cd /tmp; ./crux percolator {0:s} {1:s}".format(key, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  done = False
  while not done:
    process_output = str(subprocess.check_output("ps aux | grep crux", shell=True))
    done = len(process_output.split("\n")) == 1
    time.sleep(1)

  print(subprocess.check_output("ls -l {0:s}".format(output_dir), shell=True))
  output_bucket = params["output_bucket"]
  for item in ["target.psms", "decoy.psms", "target.peptides", "decoy.peptides"]:
    input_file = "{0:s}/percolator.{1:s}.txt".format(output_dir, item)
    output_file = "percolator.{0:s}.{1:f}.txt".format(item, ts)
    s3.Object(output_bucket, output_file).put(Body=open(input_file, 'rb'))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("percolator.json").read())
  run_percolator(bucket_name, key, params)

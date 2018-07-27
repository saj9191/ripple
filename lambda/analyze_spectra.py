import boto3
import json
import os
import subprocess
import util


def analyze_spectra(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))

  s3 = boto3.resource('s3')

  database_bucket = s3.Bucket("maccoss-human-fasta")
  output_bucket = s3.Bucket(params["output_bucket"])
  num_threads = params["num_threads"]

  with open("/tmp/HUMAN.fasta.20170123", "wb") as f:
    database_bucket.download_fileobj("HUMAN.fasta.20170123", f)

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  input_bucket = s3.Bucket(bucket_name)
  with open("/tmp/{0:s}".format(key), "wb") as f:
    input_bucket.download_fileobj(key, f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  index_files = ["auxlocs", "pepix", "protix"]
  if not os.path.isdir("/tmp/HUMAN.fasta.20170123.index"):
    os.mkdir("/tmp/HUMAN.fasta.20170123.index")

  for index_file in index_files:
    with open("/tmp/HUMAN.fasta.20170123.index/{0:s}".format(index_file), "wb") as f:
      database_bucket.download_fileobj(index_file, f)

  ts = m["timestamp"]
  output_dir = "/tmp/crux-output-{0:f}".format(ts)

  arguments = [
    "--num-threads", str(num_threads),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
    "--overwrite", "T"
  ]

  command = "cd /tmp; ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(key, " ".join(arguments))
  try:
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    new_key = util.file_name(ts, nonce, m["file_id"], m["id"], m["max_id"], "txt")
    output_file = "{0:s}/tide-search.txt".format(output_dir)
    if os.path.isfile(output_file):
      output = open(output_file).read()
      output_bucket.put_object(Key=new_key, Body=str.encode(output))
    else:
      print("ERROR", output_file, "does not exist")
  except subprocess.CalledProcessError as e:
    print(e.output)
    if "Segmentation fault" in e.output.decode("utf-8"):
      print("Too much data!")
      raise e
    else:
      raise e


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]

  params = json.loads(open("params.json").read())
  analyze_spectra(bucket_name, key, params)

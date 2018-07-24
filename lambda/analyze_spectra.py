import boto3
import constants
import json
import os
import shutil
import spectra
import subprocess
import util
import xml.etree.ElementTree as ET

ET.register_namespace("", constants.XML_NAMESPACE)


def split_spectra(s3, bucket_name, key):
  root = ET.parse("/tmp/{0:s}".format(key)).getroot()
  s = root[0][4][0]
  half = int(len(s) / 2)

  m = util.parse_file_name(key)
  bucket = s3.Bucket(bucket_name)

  first_half = s[:half]
  new_key = util.file_name(m["timestamp"], m["nonce"], m["file_id"], m["id"], m["max_id"], m["ext"], 1)
  bucket.put_object(Key=new_key, Body=str.encode(spectra.mzMLSpectraIterator.create(first_half)))

  second_half = s[half:]
  new_key = util.file_name(m["timestamp"], m["nonce"], m["file_id"], m["id"], m["max_id"], m["ext"], 2)
  bucket.put_object(Key=new_key, Body=str.encode(spectra.mzMLSpectraIterator.create(second_half)))


def analyze_spectra(bucket_name, key, start_byte, end_byte, file_id, more, params):
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

  subset_key = "{0:f}-{1:d}-{2:d}.{3:s}".format(ts, start_byte, end_byte, m["ext"])
  obj = s3.Object(bucket_name, key)
  print(bucket_name, key)

  with open("/tmp/{0:s}".format(subset_key), "wb") as f:
    content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8").strip()
    index = content.rindex(constants.CLOSING_TAG)
    content = content[:index + len(constants.CLOSING_TAG)]
    root = ET.fromstring("<data>" + content + "</data>")
    f.write(str.encode(spectra.mzMLSpectraIterator.create(list(root.iter("spectrum")))))

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

  command = "cd /tmp; ./crux tide-search {0:s} HUMAN.fasta.20170123.index {1:s}".format(subset_key, " ".join(arguments))
  try:
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    if more:
      byte_id = start_byte
    else:
      byte_id = obj.content_length

    new_key = util.file_name(ts, nonce, file_id, byte_id, obj.content_length, "txt")
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
      shutil.rmtree(output_dir)
      split_spectra(s3, bucket_name, key)
    else:
      raise e


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  start_byte = event["Records"][0]["s3"]["range"]["start_byte"]
  start_byte = event["Records"][0]["s3"]["range"]["start_byte"]
  end_byte = event["Records"][0]["s3"]["range"]["end_byte"]
  end_byte = event["Records"][0]["s3"]["range"]["end_byte"]
  file_id = event["Records"][0]["s3"]["range"]["file_id"]
  more = event["Records"][0]["s3"]["range"]["more"]

  params = json.loads(open("analyze_spectra.json").read())
  analyze_spectra(bucket_name, key, start_byte, end_byte, file_id, more, params)

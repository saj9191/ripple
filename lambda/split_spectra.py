import boto3
import json
import re
import subprocess
import time
import util

SPECTRA = re.compile("S\s")

def save_spectra(output_bucket, header, spectra, ts, end_bytes, num_bytes, num_files, final):
  key = "spectra-{0:f}-{1:d}-{2:d}-{3:d}.ms2".format(ts, num_files, end_bytes, num_bytes)
  output_bucket.put_object(Key=key, Body=str.encode("{0:s}\n{1:s}".format(header, spectra)))

def split_spectra(bucket_name, key, batch_size, chunk_size):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket("maccoss-human-split-spectra")

  obj = s3.Object(bucket_name, key)
  num_bytes = obj.content_length
  ts = time.time()
  spectra = []
  remainder = ""
  header = None

  start_byte = 0
  num_files = 0

  while start_byte < num_bytes:
    end_byte = min(start_byte + chunk_size, num_bytes)
    stream = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")
    stream = remainder + stream
    if start_byte == 0:
      parts = stream.split("\n")
      header = parts[0]
      stream = "\n".join(parts[1:])
    parts = SPECTRA.split(stream)
    parts = list(filter(lambda p: len(p) > 0, parts))
    if len(parts) > 1:
      spectra += parts[:-1]
      while len(spectra) >= batch_size:
        save_spectra(output_bucket, header, "S ".join(spectra[:batch_size]), ts, end_byte, num_bytes, num_files, False)
        spectra = spectra[batch_size:]
        num_files += 1

    remainder = parts[-1]
    start_byte = end_byte + 1

  print("number of files", num_files)
  parts = spectra + [remainder]
  save_spectra(output_bucket, header, "S ".join(spectra), ts, num_bytes, num_bytes, num_files, True)

def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params["batch_size"], params["chunk_size"])

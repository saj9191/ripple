import boto3
import json
import re
import subprocess
import time
import util

SPECTRA = re.compile("S\s\d+.*")

def save_spectra(output_bucket, spectra, ts, num_files, i):
  key = "spectra-{0:f}-{1:d}-{2:d}.ms2".format(ts, num_files, i)
  output_bucket.put_object(Key=key, Body=str.encode("\n".join(spectra)))

def split_spectra(bucket_name, key, batch_size):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket("maccoss-human-split-spectra")

  spectra = s3.Object(bucket_name, key).get()["Body"].read().decode("utf-8")

  spectrum = []
  spectra_subset = []
  ts = time.time()

  i = 0
  lines = spectra.split("\n")
  num_spectra = list(filter(lambda line: "MS1Intensity" in line, lines))
  num_files = int((len(num_spectra) + batch_size - 1) / batch_size)

  print("There are", len(lines), "of files spectra")
  for line in lines:
    if SPECTRA.match(line):
      if len(spectrum) > 0:
        spectra_subset.append("\n".join(spectrum))

      spectrum = []
      if len(spectra_subset) > batch_size:
        save_spectra(output_bucket, spectra_subset, ts, num_files, i)
        spectra_subset = []
        i += 1

    spectrum.append(line)

  spectra_subset.append("\n".join(spectrum))
  save_spectra(output_bucket, spectra_subset, ts, num_files, i)

def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params["batch_size"])


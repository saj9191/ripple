import boto3
import re
import subprocess
import time

SPECTRA = re.compile("S\s\d+.*")

def save_spectra(output_bucket, spectra, ts, num_files, i):
  key = "spectra-{0:f}-{1:d}-{2:d}.ms2".format(ts, num_files, i)
  output_bucket.put_object(Key=key, Body=str.encode("\n".join(spectra)))

def split_spectra(bucket_name, key, spectra_per_file):
  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket("maccoss-human-split-spectra")

  print("Reading")
  spectra = s3.Object(bucket_name, key).get()["Body"].read().decode("utf-8")
  print("Read")

  spectrum = []
  spectra_subset = []
  ts = time.time()

  i = 0
  lines = spectra.split("\n")
  num_spectra = list(filter(lambda line: "MS1Intensity" in line, lines))
  num_files = int((len(num_spectra) + spectra_per_file - 1) / spectra_per_file)
  
  print("There are", len(lines), "of files spectra")
  for line in lines:
    if SPECTRA.match(line):
      if len(spectrum) > 0:
        spectra_subset.append("\n".join(spectrum))
        
      spectrum = []
      if len(spectra_subset) > spectra_per_file:
        save_spectra(output_bucket, spectra_subset, ts, num_files, i)
        spectra_subset = []
        i += 1

    spectrum.append(line)
    
  spectra_subset.append("\n".join(spectrum))
  save_spectra(output_bucket, spectra_subset, ts, num_files, i)

def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  spectra_per_file = 1000
  split_spectra(bucket_name, key, spectra_per_file)


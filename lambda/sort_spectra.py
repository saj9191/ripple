import boto3
import json
import re
import util


def sort_spectra(bucket_name, key, params):
  util.clear_tmp()
  s3 = boto3.resource("s3")

  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read().decode("utf-8")
  lines = content.split("\n")

  spectra = []
  start_index = 0

  while start_index != -1:
    [mass, spectrum, start_index] = util.get_next_spectra(lines, start_index)
    if mass != -1:
      spectra.append((mass, spectrum))

  spectra.sort(key=util.getMass)

  sorted_name = "sorted-{0:s}".format(key)
  f = open("/tmp/{0:s}".format(sorted_name), "w+")
  for spectrum in spectra:
    f.write(spectrum[1] + "\n")
  f.close()

  s3.Object(params["output_bucket"], key).put(Body=open("/tmp/{0:s}".format(sorted_name), 'rb'))
  obj = s3.Object(params["output_bucket"], key)
  content = obj.get()["Body"].read().decode("utf-8")
  after = content.split("\n")
  print(len(lines), len(after))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("sort_spectra.json").read())
  sort_spectra(bucket_name, key, params)

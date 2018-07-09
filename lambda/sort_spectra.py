import boto3
import re
import util


INPUT_FILE = util.spectra_regex("ms2")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")


def sort_spectra(bucket_name, key):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  output_bucket_name = "maccoss-human-sort-spectra"

  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read().decode("utf-8")
  lines = content.split("\n")[1:]

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
    f.write(spectrum)

  f.close()

  s3.Object(output_bucket_name, sorted_name).put(Body=open("/tmp/{0:s}".format(sorted_name), 'rb'))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  sort_spectra(bucket_name, key)

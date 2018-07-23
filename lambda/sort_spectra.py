import boto3
import json
import sort
import util


def sort_spectra(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f}".format(m["timestamp"]))

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read().decode("utf-8")
  spectra = sort.sort(content)

  sorted_name = "sorted-{0:s}".format(key)
  f = open("/tmp/{0:s}".format(sorted_name), "w+")
  for spectrum in spectra:
    f.write(spectrum[1])
  f.close()

  s3.Object(params["output_bucket"], key).put(Body=open("/tmp/{0:s}".format(sorted_name), 'rb'))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("sort_spectra.json").read())
  sort_spectra(bucket_name, key, params)

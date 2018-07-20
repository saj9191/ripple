import json
import split
import util


def split_spectra(bucket_name, key, params):
  util.clear_tmp()
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]
  split.split_spectra(key, bucket_name, batch_size, chunk_size)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params)

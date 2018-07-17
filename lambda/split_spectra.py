import boto3
import json
import spectra
import util


def split_spectra(bucket_name, key, params):
  util.clear_tmp()
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket(params["output_bucket"])

  obj = s3.Object(bucket_name, key)
  num_bytes = obj.content_length

  m = util.parse_file_name(key)
  ts = m["timestamp"]
  ext = m["ext"]
  print("ext", m["ext"])

  if ext == "mzML":
    iterator = spectra.mzMLSpectraIterator(obj, batch_size, chunk_size)
  elif ext == "ms2":
    iterator = spectra.ms2SpectraIterator(obj, batch_size, chunk_size)

  more = True
  file_id = 1
  while more:
    [s, more] = iterator.nextFile()
    if more:
      byte_id = file_id
    else:
      byte_id = num_bytes
    key = util.file_name(ts, file_id, byte_id, num_bytes, ext)
    output_bucket.put_object(Key=key, Body=str.encode(s))
    file_id += 1


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params)

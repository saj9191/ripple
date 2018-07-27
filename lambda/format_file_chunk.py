import boto3
import importlib
import json
import util


def format_file_chunk(bucket_name, key, file_id, start_byte, end_byte, more, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  nonce = m["nonce"]
  timestamp = m["timestamp"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(timestamp, nonce))

  s3 = boto3.resource('s3')
  obj = s3.Object(bucket_name, key)
  format_lib = importlib.import_module(params["format"])
  iterator_class = getattr(format_lib, "Iterator")
  content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8").strip()

  output_bucket = s3.Bucket(params["output_bucket"])
  m["last"] = not more
  new_key = util.file_name(m)
  output_bucket.put_object(Key=new_key, Body=iterator_class.createContent(content))


def handler(event, context):
  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]
  file_id = s3["range"]["file_id"]
  start_byte = s3["range"]["start_byte"]
  end_byte = s3["range"]["end_byte"]
  more = s3["range"]["more"]
  params = json.loads(open("params.json").read())
  format_file_chunk(bucket_name, key, file_id, start_byte, end_byte, more, params)

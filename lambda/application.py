import boto3
import importlib
import json
import util


def run_application(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))

  s3 = boto3.resource('s3')
  input_bucket = s3.Bucket(bucket_name)

  temp_file = "/tmp/{0:s}".format(key)
  print(bucket_name, key)
  with open(temp_file, "wb") as f:
    input_bucket.download_fileobj(key, f)

  application_lib = importlib.import_module(params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(temp_file, params, m)

  output_bucket = s3.Bucket(params["output_bucket"])

  for output_file in output_files:
    index = output_file.rfind("/")
    if index != -1:
      file_name = output_file[index + 1:]
    else:
      file_name = output_file
    index = file_name.rfind(".")
    prefix = file_name[:index]
    ext = file_name[index+1:]
    new_key = util.file_name(ts, nonce, m["file_id"], m["id"], m["max_id"], ext, prefix=prefix)
    output_bucket.put_object(Key=new_key, Body=open(output_file, "rb"))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]

  params = json.loads(open("params.json").read())
  run_application(bucket_name, key, params)

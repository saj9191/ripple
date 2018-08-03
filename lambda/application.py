import boto3
import importlib
import json
import util


def run_application(bucket_name, key, params):
  util.clear_tmp()
  print("bucket", bucket_name, "key", key)
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d} FILE {2:d}".format(ts, nonce, m["file-id"]))

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

    p = util.parse_file_name(file_name)
    if p is None:
      index = file_name.rfind(".")
      prefix = file_name[:index]
      ext = file_name[index+1:]
      m["prefix"] = prefix
      m["ext"] = ext
      new_key = util.file_name(m)
    else:
      new_key = file_name
    output_bucket.put_object(Key=new_key, Body=open(output_file, "rb"))


def handler(event, context):
  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]
  params = json.loads(open("params.json").read())
  if "extra_params" in s3:
    params["extra_params"] = s3["extra_params"]
  print("params", params)
  run_application(bucket_name, key, params)

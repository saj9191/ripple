import boto3
import importlib
import json
import util


def run_application(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
#  util.print_request(m, params)

  s3 = boto3.resource('s3')
  input_bucket = s3.Bucket(bucket_name)

  temp_file = "/tmp/{0:s}".format(key)
  with open(temp_file, "wb") as f:
    input_bucket.download_fileobj(key, f)

  application_lib = importlib.import_module(params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(temp_file, params, m)

  output_bucket = s3.Bucket(params["output_bucket"])

  for output_file in output_files:
    index = output_file.rfind("/")
    file_name = output_file[index + 1:] if index != -1 else output_file

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
    util.print_write(m, new_key, params)
    output_bucket.put_object(Key=new_key, Body=open(output_file, "rb"))


def handler(event, context):
  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]
  params = json.loads(open("params.json").read())
  params["request_id"] = context.aws_request_id
  if "extra_params" in s3:
    params["extra_params"] = s3["extra_params"]
  run_application(bucket_name, key, params)

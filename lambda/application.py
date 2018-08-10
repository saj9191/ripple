import boto3
import importlib
import util


def run_application(bucket_name, key, input_format, output_format, start_byte, end_byte, params):
  s3 = boto3.resource('s3')
  input_bucket = s3.Bucket(bucket_name)

  temp_file = "/tmp/{0:s}".format(key)
  with open(temp_file, "wb") as f:
    input_bucket.download_fileobj(key, f)

  application_lib = importlib.import_module(params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(temp_file, params, input_format, output_format)

  output_bucket = s3.Bucket(params["bucket"])
  for output_file in output_files:
    index = output_file.rfind("/")
    file_name = output_file[index + 1:] if index != -1 else output_file

    p = util.parse_file_name(file_name)
    if p is None:
      index = file_name.rfind(".")
      ext = file_name[index+1:]
      output_format["ext"] = ext
      new_key = util.file_name(output_format)
    else:
      new_key = file_name

    util.print_write(output_format, new_key, params)
    output_bucket.put_object(Key=new_key, Body=open(output_file, "rb"))


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, run_application)
  util.show_duration(context, m, params)

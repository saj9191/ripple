import argparse
import json
import os
import shutil
import subprocess
import util


def upload_lambda(client, fparams, files):
  f = open("params.json", "w")
  f.write(json.dumps(fparams))
  f.close()

  files.append("params.json")

  subprocess.call("zip {0:s}.zip {1:s}".format(fparams["file"], " ".join(files)), shell=True)

  with open("{0:s}.zip".format(fparams["file"]), "rb") as f:
    zipped_code = f.read()

  response = client.update_function_code(
    FunctionName=fparams["name"],
    ZipFile=zipped_code,
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
  response = client.update_function_configuration(
    FunctionName=fparams["name"],
    Timeout=fparams["timeout"],
    MemorySize=fparams["memory_size"]
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
  os.remove("{0:s}.zip".format(fparams["file"]))


def upload_split_file(client, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "iterator.py",
    "split_file.py",
    "util.py",
  ]
  upload_lambda(client, fparams, files)
  os.remove(format_file)


def upload_format_file_chunk_file(client, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "format_file_chunk.py",
    "header.{0:s}".format(fparams["format"]),
    "iterator.py",
    "util.py",
  ]
  upload_lambda(client, fparams, files)
  os.remove(format_file)


def upload_application_file(client, fparams):
  files = [
    "application.py",
    "util.py",
  ]

  file = "{0:s}.py".format(fparams["application"])
  shutil.copyfile("../{0:s}".format(file), file)

  files.append(file)
  upload_lambda(client, fparams, files)
  os.remove(file)


def upload_combine_files(client, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  files = [
    format_file,
    "iterator.py",
    "combine_files.py",
    "util.py",
  ]
  upload_lambda(client, fparams, files)
  os.remove(format_file)


def upload_functions(client, params):
  common_files = [
    "constants.py",
    "header.mzML",
    "iterator.py",
    "util.py",
  ]
  for file in common_files:
    shutil.copyfile(file, "lambda/{0:s}".format(file))

  os.chdir("lambda")
  for fparams in params["pipeline"]:
    if fparams["file"] == "split_file":
      upload_split_file(client, fparams)
    elif fparams["file"] == "format_file_chunk":
      upload_format_file_chunk_file(client, fparams)
    elif fparams["file"] == "combine_files":
      upload_combine_files(client, fparams)
    elif fparams["file"] == "application":
      upload_application_file(client, fparams)

  os.chdir("..")
  for file in common_files:
    os.remove("lambda/{0:s}".format(file))


def setup_notifications(client, bucket, config):
  response = client.put_bucket_notification_configuration(
    Bucket=bucket,
    NotificationConfiguration=config
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)


def clear_triggers(client, params):
  for bucket in params["buckets"]:
    setup_notifications(client, bucket, {})


def setup_triggers(params):
  client = util.setup_client("s3", params)
  clear_triggers(client, params)
  for function in params["pipeline"]:
    if "input_bucket" in function:
      config = {
        "LambdaFunctionConfigurations": [{
          "LambdaFunctionArn": params["lambda"][function["name"]]["arn"],
          "Events": ["s3:ObjectCreated:*"],
          "Filter": {
            "Key": {
              "FilterRules": [{
                "Name": "prefix",
                "Value": ""
              }]
            }
          }
        }]
      }
      print(function, function["input_bucket"], config)
      setup_notifications(client, function["input_bucket"], config)


def setup(params):
  if params["model"] != "ec2":
    client = util.create_client(params)
    upload_functions(client, params)

  #setup_triggers(params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials(args.parameters.split(".")[0].split("/")[1])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  setup(params)


if __name__ == "__main__":
  main()

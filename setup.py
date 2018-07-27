import argparse
import json
import os
import shutil
import subprocess
import util


def upload_lambda(client, fparams, files):
  print("zip {0:s}.zip {1:s}".format(fparams["file"], " ".join(files)))
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
  f = open("params.json", "w")
  f.write(json.dumps(fparams))
  f.close()

  files = [
    "{0:s}.py".format(fparams["format"]),
    "constants.py",
    "iterator.py",
    "params.json",
    "split_file.py",
    "util.py",
  ]
  upload_lambda(client, fparams, files)


def upload_functions(client, params):
  common_files = [
    "iterator.py",
    "mzML.py",
    "util.py",
  ]
  for file in common_files:
    shutil.copyfile(file, "lambda/{0:s}".format(file))

  os.chdir("lambda")
  for section in params["pipeline"]:
    if section["file"] == "split_file":
      upload_split_file(client, section)

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
    print(function, config)
    setup_notifications(client, function["input_bucket"], config)


def setup(params):
  if params["model"] != "ec2":
    client = util.create_client(params)
    upload_functions(client, params)

  setup_triggers(params)


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

import argparse
import botocore
import json
import os
import shutil
import subprocess
import util


def upload_function_code(client, zip_file, fparams, params, create):
  zipped_code = open(zip_file, "rb").read()
  if create:
    response = client.create_function(
      FunctionName=fparams["name"],
      Runtime="python3.6",
      Role="arn:aws:iam::469290334000:role/LambdaExecution",
      Handler="{0:s}.handler".format(fparams["file"]),
      Code={
        "ZipFile": zipped_code
      },
      Timeout=params["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 201)

  else:
    response = client.update_function_code(
      FunctionName=fparams["name"],
      ZipFile=zipped_code
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    response = client.update_function_configuration(
      FunctionName=fparams["name"],
      Timeout=params["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    try:
      client.remove_permission(
        FunctionName=fparams["name"],
        StatementId=fparams["name"] + "-" + params["bucket"]
      )
    except Exception as e:
      print("Cannot remove permissions for", fparams["name"], params["bucket"])


def create_parameter_files(zip_directory, function_name, params):
  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    if pparams["name"] == function_name:
      json_name = "{0:s}/{1:d}.json".format(zip_directory, i)
      f = open(json_name, "w")
      f.write(json.dumps(pparams))
      f.close()


def upload_functions(client, params):
  zip_directory = "lambda_dependencies"
  zip_file = "lambda.zip"

  response = client.list_functions()
  functions = set(list(map(lambda f: f["FunctionName"], response["Functions"])))

  for fparams in params["functions"]:
    os.makedirs(zip_directory)
    files = []
    file = "{0:s}.py".format(fparams["file"])
    shutil.copyfile("lambda/{0:s}".format(file), "{0:s}/{1:s}".format(zip_directory, file))
    files.append(file)
    for file in params["dependencies"]["common"]:
      index = file.rfind("/")
      name = file[index + 1:]
      files.append(name)
      shutil.copyfile(file, "{0:s}/{1:s}".format(zip_directory, name))

    if "format" in fparams:
      name = "{0:s}.py".format(fparams["format"])
      shutil.copyfile("formats/{0:s}".format(name),  name)

    create_parameter_files(zip_directory, fparams["name"], params)
    os.chdir(zip_directory)
    subprocess.call("zip ../{0:s} {1:s}".format(zip_file, " ".join(files)), shell=True)
    os.chdir("..")

    upload_function_code(client, zip_file, fparams, params, fparams["name"] not in functions)
    shutil.rmtree(zip_directory)


def setup_notifications(client, bucket, config):
  response = client.put_bucket_notification_configuration(
    Bucket=bucket,
    NotificationConfiguration=config
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)


def clear_triggers(client, bucket, params):
  setup_notifications(client, bucket, {})


def create_bucket(client, bucket_name, params):
  try:
    client.create_bucket(
      ACL="public-read-write",
      Bucket=bucket_name,
      CreateBucketConfiguration={
        "LocationConstraint": params["region"],
      }
    )
  except botocore.exceptions.ClientError as ex:
    if "BucketAlreadyOwnedByYou" not in str(ex):
      raise ex


def function_buckets(params):
  buckets = []
  if "input_bucket" in params:
    buckets.append(params["input_bucket"])
  elif "bucket_prefix" in params:
    for i in range(params["num_buckets"]):
      buckets.append("{0:s}-{1:d}".format(params["bucket_prefix"], i + 1))
  return buckets


def get_needed_buckets(params):
  buckets = [params["pipeline"][-1]["output_bucket"]]
  for function in params["pipeline"]:
    buckets += function_buckets(function)
  return set(buckets)


def function_arns(params):
  name_to_arn = {}
  client = util.lambda_client(params)
  for function in client.list_functions()["Functions"]:
    name_to_arn[function["FunctionName"]] = function["FunctionArn"]
  return name_to_arn


def setup_triggers(params):
  name_to_arn = function_arns(params)

  prefixes = {}
  for fparams in params["functions"]:
    prefixes[fparams["name"]] = []

  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    if i == 0 or "output_function" not in params["pipeline"][i - 1]:
      prefixes[pparams["name"]].append(i)

  client = util.setup_client("s3", params)
  lambda_client = util.lambda_client(params)
  configurations = []
  for fparams in params["functions"]:
    args = {
      "FunctionName": fparams["name"],
      "StatementId": fparams["name"] + "-" + params["bucket"],
      "Action": "lambda:InvokeFunction",
      "Principal": "s3.amazonaws.com",
      "SourceAccount": "469290334000",
      "SourceArn": "arn:aws:s3:::shjoyner-tide",
    }
    lambda_client.add_permission(**args)

    for prefix in prefixes[fparams["name"]]:
      configurations.append({
        "LambdaFunctionArn": name_to_arn[fparams["name"]],
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{"Name": "prefix", "Value": "{0:d}-".format(prefix)}]
          }
        }
      })

  config = {
    "LambdaFunctionConfigurations": configurations,
  }
  setup_notifications(client, params["bucket"], config)


def setup(params):
  s3 = util.setup_client("s3", params)
  create_bucket(s3, params["bucket"], params)
  clear_triggers(s3, params["bucket"], params)

  client = util.lambda_client(params)
  upload_functions(client, params)
  setup_triggers(params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  setup(params)


if __name__ == "__main__":
  main()

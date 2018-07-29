import argparse
import json
import os
import shutil
import subprocess
import util


def upload_lambda(client, functions, fparams, files):
  json_name = "params.json"
  f = open(json_name, "w")
  f.write(json.dumps(fparams))
  f.close()

  files.append(json_name)

  subprocess.call("zip {0:s}.zip {1:s}".format(fparams["file"], " ".join(files)), shell=True)

  with open("{0:s}.zip".format(fparams["file"]), "rb") as f:
    zipped_code = f.read()

  buckets = function_buckets(fparams)
  if fparams["name"] in functions:
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
    for bucket in buckets:
      try:
        client.remove_permission(
          FunctionName=fparams["name"],
          StatementId=fparams["name"] + "-" + bucket
        )
      except Exception as e:
        print("Cannot remove permissions for", fparams["name"], bucket)

  else:
    response = client.create_function(
      FunctionName=fparams["name"],
      Runtime="python3.6",
      Role="arn:aws:iam::469290334000:role/LambdaExecution",
      Handler="{0:s}.handler".format(fparams["file"]),
      Code={
        "ZipFile": zipped_code
      },
      Timeout=fparams["timeout"],
      MemorySize=fparams["memory_size"]
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 201)

  for bucket in buckets:
    client.add_permission(
      FunctionName=fparams["name"],
      StatementId=fparams["name"] + "-" + bucket,
      Action="lambda:InvokeFunction",
      Principal="s3.amazonaws.com",
      SourceArn="arn:aws:s3:::{0:s}".format(bucket)
    )

  os.remove("{0:s}.zip".format(fparams["file"]))
  os.remove(json_name)


def upload_format_file(client, functions, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "header.{0:s}".format(fparams["format"]),
    "iterator.py",
    "{0:s}.py".format(fparams["file"]),
    "util.py"
  ]

  shutil.copyfile("../formats/pivot.py", "pivot.py")
  files.append("pivot.py")

  print(files, fparams["name"])
  upload_lambda(client, functions, fparams, files)
  os.remove(format_file)
  os.remove("pivot.py")


def upload_format_file_chunk_file(client, functions, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "format_file_chunk.py",
    "header.{0:s}".format(fparams["format"]),
    "iterator.py",
    "util.py",
  ]
  upload_lambda(client, functions, fparams, files)
  os.remove(format_file)


def upload_application_file(client, functions, fparams):
  files = [
    "application.py",
    "util.py",
  ]

  file = "{0:s}.py".format(fparams["application"])
  shutil.copyfile("../applications/{0:s}".format(file), file)

  files.append(file)
  upload_lambda(client, functions, fparams, files)
  os.remove(file)


def upload_combine_files(client, functions, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "header.{0:s}".format(fparams["format"]),
    "iterator.py",
    "combine_files.py",
    "util.py",
  ]
  upload_lambda(client, functions, fparams, files)
  os.remove(format_file)


def upload_pivot_file(client, functions, fparams):
  format_file = "{0:s}.py".format(fparams["format"])
  shutil.copyfile("../formats/{0:s}".format(format_file), format_file)
  files = [
    format_file,
    "iterator.py",
    "util.py",
    "pivot_file.py",
  ]

  upload_lambda(client, functions, fparams, files)
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

  response = client.list_functions()
  functions = set(list(map(lambda f: f["FunctionName"], response["Functions"])))

  os.chdir("lambda")
  for fparams in params["pipeline"]:
    if fparams["file"] == "format_file_chunk":
      upload_format_file_chunk_file(client, functions, fparams)
    elif fparams["file"] == "combine_files":
      upload_combine_files(client, functions, fparams)
    elif fparams["file"] == "application":
      upload_application_file(client, functions, fparams)
    elif fparams["file"] == "pivot_file":
      upload_pivot_file(client, functions, fparams)
    else:
      upload_format_file(client, functions, fparams)

  os.chdir("..")
  for file in common_files:
    os.remove("lambda/{0:s}".format(file))


def setup_notifications(client, bucket, config):
  response = client.put_bucket_notification_configuration(
    Bucket=bucket,
    NotificationConfiguration=config
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)


def clear_triggers(client, buckets, params):
  for bucket in buckets:
    setup_notifications(client, bucket, {})


def create_bucket(client, bucket_name, params):
  print("Creating bucket", bucket_name)
  client.create_bucket(
    ACL="public-read-write",
    Bucket=bucket_name,
    CreateBucketConfiguration={
      "LocationConstraint": params["region"],
    }
  )


def function_buckets(params):
  buckets = []
  if "input_bucket" in params:
    buckets.append(params["input_bucket"])
  elif "bucket_prefix" in params:
    for i in range(params["num_bins"]):
      buckets.append("{0:s}-{1:d}".format(params["bucket_prefix"], i + 1))
  return buckets


def get_needed_buckets(params):
  buckets = [params["pipeline"][-1]["output_bucket"]]
  for function in params["pipeline"]:
    buckets += function_buckets(function)
  return set(buckets)


def setup_triggers(params):
  name_to_arn = {}
  client = util.setup_client("lambda", params)
  for function in client.list_functions()["Functions"]:
    name_to_arn[function["FunctionName"]] = function["FunctionArn"]

  client = util.setup_client("s3", params)
  for function in params["pipeline"]:
    buckets = function_buckets(function)
    for bucket in buckets:
      config = {
        "LambdaFunctionConfigurations": [{
          "Id": function["name"],
          "LambdaFunctionArn": name_to_arn[function["name"]],
          "Events": ["s3:ObjectCreated:*"]
        }]
      }
      setup_notifications(client, bucket, config)


def setup(params):
  client = util.setup_client("s3", params)
  client_buckets = client.list_buckets()["Buckets"]
  existing_buckets = set(list(map(lambda b: b["Name"], client_buckets)))

  needed_buckets = get_needed_buckets(params)
  clear_triggers(client, existing_buckets.intersection(needed_buckets), params)
  for bucket in needed_buckets:
    if bucket not in existing_buckets:
      create_bucket(client, bucket, params)
      existing_buckets.add(bucket)

  client = util.create_client(params)
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

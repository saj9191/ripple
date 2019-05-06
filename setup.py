import abc
import argparse
import boto3
import botocore
import json
import os
import shutil
import subprocess
import util


class Setup:
  def __init__(self, params):
    self.params = params

  def __create_parameter_files__(self, zip_directory, function_name):
    for i in range(len(self.params["pipeline"])):
      pparams = self.params["pipeline"][i]
      if pparams["name"] == function_name:
        p = {**self.params["functions"][function_name], **pparams}
        for value in ["timeout", "num_bins", "bucket", "storage_class", "log", "scheduler"]:
          if value in self.params:
            p[value] = self.params[value]

        name = "{0:d}.json".format(i)
        json_path = "{0:s}/{1:s}".format(zip_directory, name)
        f = open(json_path, "w")
        f.write(json.dumps(p))
        f.close()

  def __copy_file__(self, directory, file_path):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    index = file_path.rfind("/")
    file_name = file_path[index + 1:]
    shutil.copyfile(dir_path + "/" + file_path, "{0:s}/{1:s}".format(directory, file_name))
    return file_name

  # Creates a table / bucket to load data to.
  @abc.abstractmethod
  def __create_table__(self, name):
    raise Exception("Setup::__create_table__ not implemented")

  # Returns a list of function names currently uploaded to provider.
  @abc.abstractmethod
  def __get_functions__(self):
    raise Exception("Setup::__create_table__ not implemented")

  # Setup the account credientials
  @abc.abstractmethod
  def __setup_credentials__(self):
    raise Exception("Setup::__setup_credentials__ not implemented")

  def __setup_function__(self, name, create):
    zip_directory = "lambda_dependencies"
    zip_file = "lambda.zip"

    if os.path.isdir(zip_directory):
      shutil.rmtree(zip_directory)

    function_params = self.params["functions"][name]
    os.makedirs(zip_directory)

    self.__zip_ripple_file__(zip_directory, function_params)
    self.__zip_application__(zip_directory, function_params)
    self.__zip_formats__(zip_directory, function_params)
    self.__create_parameter_files__(zip_directory, name)
    os.chdir(zip_directory)
    subprocess.call("zip -r9 ../{0:s} .".format(zip_file), shell=True)
    os.chdir("..")
    self.__upload_function__(name, zip_file, function_params, create)
    os.remove(zip_file)
    shutil.rmtree(zip_directory)

  def __setup_functions__(self):
    functions = self.__get_functions__()
    for name in self.params["functions"]:
      self.__setup_function__(name, name not in functions) 

  # Setup serverless triggers on the table
  @abc.abstractmethod
  def __setup_table_notifications__(table_name):
    raise Exception("Setup::__setup_table_notifications__ not implemented")

  # Creates the user role with the necessary permissions to execute the pipeline. 
  # This includes function and table permissions.
  @abc.abstractmethod
  def __setup_user_permissions__(self):
    raise Exception("Setup::__setup_user_permissions__ not implemented")

  # Uploads the code for the function and sets up the triggers.
  @abc.abstractmethod
  def __upload_function__(self, name, zip_file, create):
    raise Exception("Setup::__upload_function__ not implemented")

  def __zip_application__(self, zip_directory, fparams):
    if "application" in fparams:
      self.__copy_file__(zip_directory, "applications/{0:s}.py".format(fparams["application"]))

  def __zip_formats__(self, zip_directory, fparams):
    if "format" in fparams:
      form = fparams["format"]
      if "dependencies" in self.params and form in self.params["dependencies"]["formats"]:
        for file in self.params["dependencies"]["formats"][form]:
          self.__copy_file__(zip_directory, file)
      self.__copy_file__(zip_directory, "formats/{0:s}.py".format(form))

  def __zip_ripple_file__(self, zip_directory, fparams):
    file = "{0:s}.py".format(fparams["file"])
    dir_path = os.path.dirname(os.path.realpath(__file__))
    shutil.copyfile(dir_path + "/lambda/{0:s}".format(file), "{0:s}/{1:s}".format(zip_directory, file))
    for file in ["formats/iterator.py", "formats/pivot.py", "database.py", "util.py"]:
      self.__copy_file__(zip_directory, file)
    for format in fparams["formats"]:
      self.__copy_file__(zip_directory, "formats/{0:s}.py".format(format))

  def start(self):
    self.__setup_credentials__()
    self.__setup_user_permissions__()
    self.__create_table__(self.params["bucket"])
    self.__create_table__(self.params["log"])
    self.__setup_functions__()
    self.__setup_table_notifications__(self.params["bucket"])


class LambdaSetup(Setup):
  def __init__(self, params):
    Setup.__init__(self, params)

  def __bucket_exists__(self, name):
    s3_client = boto3.client("s3")
    try:
      s3_client.head_bucket(Bucket=name)
      return True
    except botocore.exceptions.ClientError as ex:
      return False

  def __create_table__(self, name):
    s3_client = boto3.client("s3")
    if not self.__bucket_exists__(name):
      try:
        s3_client.create_bucket(
          ACL="public-read-write",
          Bucket=name,
          CreateBucketConfiguration={
            "LocationConstraint": self.params["region"],
          }
        )
      except botocore.exceptions.ClientError as ex:
        if "BucketAlreadyOwnedByYou" not in str(ex):
          raise ex
    else:
      self.__setup_table_notification__(name, {})

  def __get_functions__(self):
    lambda_client = boto3.client("lambda")
    functions = {}
    for function in lambda_client.list_functions()["Functions"]:
      functions[function["FunctionName"]] = function
    return functions

  def __get_layers__(self, region, account_id, imports):
    layers = []
    for imp in imports:
      if imp  == "PIL":
        layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:PIL:2".format(region, account_id))
      elif imp == "sklearn":
        layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:Sklearn:1".format(region, account_id))
      elif imp == "numpy":
        layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:numpy:1".format(region, account_id))
      else:
        raise Exception("Cannot find layer for", imp)
    return layers

  def __setup_credentials__(self):
    try:
      session = boto3.Session(profile_name="default")
    except botocore.exceptions.ProfileNotFound:
      access_key = input("Please specify AWS access key:").strip()
      secret_key = input("Please specify AWS secret key:").strip()
      subprocess.call("aws configure set aws_access_key_id {0:s}".format(access_key), shell=True)
      subprocess.call("aws configure set aws_secret_access_key {0:s}".format(secret_key), shell=True)

  def __setup_table_notification__(self, table_name, config):
    s3_client = boto3.client("s3")
    response = s3_client.put_bucket_notification_configuration(
      Bucket=table_name,
      NotificationConfiguration=config
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

  def __setup_table_notifications__(self, table_name):
    configurations = []
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    functions = self.__get_functions__()
    arn = functions[self.params["pipeline"][0]["name"]]["FunctionArn"]

    configurations.append({
      "LambdaFunctionArn": arn,
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "0/"}]
        }
      }
    })

    config = {
      "LambdaFunctionConfigurations": configurations,
    }
    self.__setup_table_notification__(table_name, config)

  def __setup_user_permissions__(self):
    account_id = int(boto3.client("sts").get_caller_identity().get("Account"))
    role_name = self.params["role"].split("/")[-1]
    iam_client = boto3.client("iam")
    region = self.params["region"]
    roles = list(filter(lambda r: r["RoleName"] == role_name, iam_client.list_roles()["Roles"]))
    if len(roles) == 0:
      iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps({
          "Version": "2012-10-17",
          "Statement": [{
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
              "Service": "lambda.amazonaws.com",
              "Action": "sts:AssumeRole",
            }
          }]
      }))

    extra = {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
          "s3:Put*",
          "s3:Get*",
          "s3:*MultipartUpload*",
        ],
        "Resource": "*",
      }, {
        "Effect": "Allow",
        "Action": "logs:CreateLogGroup",
        "Resource": "arn:aws:logs:{0:s}:{1:d}*".format(region, account_id)
      }, {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ],
        "Resource": [
          "arn:aws:logs:{0:s}:{1:d}:log-group:/aws/lambda/*:**".format(region, account_id)
        ]
      }, {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",
        "Resource": "arn:aws:iam::{0:d}:role/*".format(account_id)
      }]
    }
    iam_resource = boto3.resource("iam")
    iam_resource.RolePolicy(role_name, role_name).put(PolicyDocument=json.dumps(extra))

  def __upload_function__(self, name, zip_file, function_params, create):
    lambda_client = boto3.client("lambda")
    account_id = int(boto3.client("sts").get_caller_identity().get("Account"))
    zipped_code = open(zip_file, "rb").read()
    if create:
      response = lambda_client.create_function(
        FunctionName=name,
        Runtime="python3.6",
        Role="arn:aws:iam::{0:d}:role/{1:s}".format(account_id, self.params["role"]),
        Handler="{0:s}.handler".format(function_params["file"]),
        Code={
          "ZipFile": zipped_code
        },
        Timeout=self.params["timeout"],
        MemorySize=function_params["memory_size"],
        Layers=self.__get_layers__(self.params["region"], account_id, function_params["imports"])
      )
      assert(response["ResponseMetadata"]["HTTPStatusCode"] == 201)
    else:
      response = lambda_client.update_function_code(
        FunctionName=name,
        ZipFile=zipped_code
      )
      assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
      response = lambda_client.update_function_configuration(
        FunctionName=name,
        Timeout=self.params["timeout"],
        MemorySize=function_params["memory_size"],
        Layers=self.__get_layers__(self.params["region"], account_id, function_params["imports"])
      )
      assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
      try:
        statement_id = "{0:s}-{1:s}".format(name, self.params["bucket"])
        lambda_client.remove_permission(
          FunctionName=name,
          StatementId=statement_id,
        )
      except Exception as e:
        print("WARNING: Cannot remove permissions for", name)

    statement_id = "{0:s}-{1:s}".format(name, self.params["bucket"])
    args = {
      "FunctionName": name,
      "StatementId": statement_id,
      "Action": "lambda:InvokeFunction",
      "Principal": "s3.amazonaws.com",
      "SourceAccount": str(account_id),
      "SourceArn": "arn:aws:s3:::{0:s}".format(self.params["bucket"]),
    }

    try:
      lambda_client.add_permission(**args)
    except Exception as e:
      print("WARNING: Error adding function permissions")


def setup(params):
  s = LambdaSetup(params)
  s.start()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup(params)


if __name__ == "__main__":
  main()

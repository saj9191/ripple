import boto3
import botocore
import json
from setup.setup import Setup

class LambdaSetup(Setup):
  def __init__(self, params):
    Setup.__init__(self, params)

  def __add_additional_files__(self, zip_directory):
    pass

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
        create_bucket_kwargs = {}
        if self.params["region"] != "us-east-1":
          create_bucket_kwargs["CreateBucketConfiguration"] = {
            "LocationConstraint": self.params["region"],
          }
        s3_client.create_bucket(
          ACL="public-read-write",
          Bucket=name,
          **create_bucket_kwargs
        )
      except botocore.exceptions.ClientError as ex:
        if "BucketAlreadyOwnedByYou" not in str(ex):
          raise ex
    else:
      self.__setup_table_notification__(name, {})

  def __get_functions__(self):
    lambda_client = boto3.client("lambda", region_name=self.params["region"])
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
            },
            "Action": "sts:AssumeRole",
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
        Handler="main.main".format(function_params["file"]),
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


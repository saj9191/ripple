import boto3
from botocore.client import Config
import json
import os
import random
import re
import subprocess
import time


FILE_FORMAT = [{
  "name": "prefix",
  "type": "int",
}, {
  "name": "timestamp",
  "type": "float",
}, {
  "name": "nonce",
  "type": "int",
}, {
  "name": "bin",
  "type": "int",
}, {
  "name": "file_id",
  "type": "int",
}, {
  "name": "last",
  "type": "bool",
}, {
  "name": "suffix",
  "type": "alpha",
}]


def combine_instance(bucket_name, key):
  done = False
  num_attempts = 20
  keys = []
  prefix = key_prefix(key)
  count = 0
  while not done and (len(keys) == 0 or current_last_file(bucket_name, key)):
    [done, keys] = have_all_files(bucket_name, prefix)
    count += 1
    if count == num_attempts and not done:
      return [False, keys]
    time.sleep(1)

  print("Combine", done, len(keys))
  return [done and current_last_file(bucket_name, key), keys]


def run(bucket_name, key, params, func):
  clear_tmp()
  input_format = parse_file_name(key)
  output_format = dict(input_format)
  output_format["prefix"] = params["prefix"] + 1

  print_request(input_format, params)

  if "range" in params:
    rparams = params["range"]
    start_byte = rparams["start_byte"]
    end_byte = rparams["end_byte"]
    output_format["file_id"] = rparams["file_id"]
    output_format["last"] = not rparams["more"]
  else:
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    start_byte = 0
    end_byte = obj.content_length
    if "file_id" in params:
      output_format["file_id"] = params["file_id"]
      output_format["last"] = not params["more"]

  func(bucket_name, key, input_format, output_format, start_byte, end_byte, params)
  return output_format


def current_last_file(bucket_name, current_key):
  prefix = key_prefix(current_key)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  objects = list(bucket.objects.filter(Prefix=prefix))
  keys = set(list(map(lambda o: o.key, objects)))
  objects = sorted(objects, key=lambda o: [o.last_modified, o.key])
  print("last", objects[-1])
  return ((current_key not in keys) or (objects[-1].key == current_key))


def lambda_setup(event, context):
  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]
  key_fields = parse_file_name(key)

  if "extra_params" in s3 and "prefix" in s3["extra_params"]:
    prefix = s3["extra_params"]["prefix"]
  else:
    prefix = key_fields["prefix"]

  params = json.loads(open("{0:d}.json".format(prefix)).read())
  params["prefix"] = prefix
  params["token"] = random.randint(1, 100*1000*1000)
  params["request_id"] = context.aws_request_id
  params["key_fields"] = key_fields

  for value in ["range", "pivots"]:
    if value in s3:
      params[value] = s3[value]

  if "extra_params" in s3:
    params = {**params, **s3["extra_params"]}

  return [bucket_name, key, params]


def show_duration(context, m, params):
  duration = params["timeout"] * 1000 - context.get_remaining_time_in_millis()
  msg = "TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d} REQUEST ID {4:s} TOKEN {5:d} DURATION {6:d}"
  msg = msg.format(m["timestamp"], m["nonce"], m["bin"], m["file_id"], params["request_id"], params["token"], duration)
  print(msg)


def print_request(m, params):
  msg = "TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d} REQUEST ID {4:s} TOKEN {5:d}"
  msg = msg.format(m["timestamp"], m["nonce"], m["bin"], m["file_id"], params["request_id"], params["token"])
  print(msg)
  if "extra_params" in params and "token" in params["extra_params"]:
    msg += " INVOKED BY TOKEN {0:d}".format(params["extra_params"]["token"])
    print(msg)


def print_read(m, key, params):
  print_action(m, key, "READ", params)


def print_write(m, key, params):
  print_action(m, key, "WRITE", params)


def print_action(m, key, action, params):
  msg = "TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} {3:s} REQUEST ID {4:s} TOKEN {5:d} FILE NAME {6:s}"
  print(msg.format(m["timestamp"], m["nonce"], m["bin"], action, params["request_id"], params["token"], key))


def setup_client(service, params):
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client(service,
                        aws_access_key_id=params["access_key"],
                        aws_secret_access_key=params["secret_key"],
                        region_name=params["region"],
                        config=config
                        )
  return client


def key_prefix(key):
  return "-".join(key.split("-")[:4]) + "-"


def lambda_client(params):
  client = setup_client("lambda", params)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
  return client


def get_credentials(name):
  home = os.path.expanduser("~")
  f = open("{0:s}/.aws/credentials".format(home))
  lines = f.readlines()
  for i in range(len(lines)):
    header = "[{0:s}]".format(name)
    if lines[i].strip() == header:
      access_key = lines[i + 1].split("=")[1].strip()
      secret_key = lines[i + 2].split("=")[1].strip()
      return [access_key, secret_key]


def file_format(m):
  name = ""
  for part in FILE_FORMAT:
    if len(name) > 0:
      name += "-"
    if part["name"] in m:
      value = m[part["name"]]
      if part["type"] == "alpha":
        name += value
      elif part["type"] == "bool":
        name += str(int(value))
      elif part["type"] == "float":
        name += "{0:f}".format(value)
      else:
        name += str(value)
    else:
      if part["type"] == "alpha":
        name += "([A-Za-z]+)"
      elif part["type"] == "float":
        name += "([0-9\.]+)"
      elif part["type"] == "int":
        name += "([0-9]+)"
      else:
        name += "([0-1])"
  name += "."
  if "ext" in m:
    name += m["ext"]
  else:
    name += "([A-Za-z0-9]+)"

  return name


def file_name(m):
  m["created"] = time.time()
  return file_format(m)


def parse_file_name(file_name):
  regex = re.compile(file_format({}))
  m = regex.match(file_name)
  p = {}
  if m is None:
    return p

  i = 0
  for i in range(len(FILE_FORMAT)):
    part = FILE_FORMAT[i]
    name = part["name"]
    value = m.group(i+1)
    if part["type"] == "int":
      p[name] = int(value)
    elif part["type"] == "float":
      p[name] = float(value)
    elif part["type"] == "bool":
      p[name] = value == "1"
    else:
      p[name] = value

  p["ext"] = m.group(len(FILE_FORMAT) + 1)
  return p


def get_key_regex(m):
  return re.compile(file_format(m))


def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)


def have_all_files(bucket_name, prefix):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)

  num_files = None
  ids_to_keys = {}
  for key in bucket.objects.filter(Prefix=prefix):
    m = parse_file_name(key.key)
    if m["file_id"] in ids_to_keys:
      if key.key < ids_to_keys[m["file_id"]]:
        ids_to_keys[m["file_id"]] = key.key
    else:
      ids_to_keys[m["file_id"]] = key.key
    if m["last"]:
      num_files = m["file_id"]

  matching_keys = list(ids_to_keys.values())
  return (len(matching_keys) == num_files, matching_keys)

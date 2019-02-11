import boto3
import botocore
import json
import os
import random
import re
import subprocess
import time
from database import Entry, S3
from botocore.client import Config
from typing import List


FILE_FORMAT = [{
  "name": "prefix",
  "type": "int",
  "folder": True,
}, {
  "name": "timestamp",
  "type": "float",
  "folder": False,
}, {
  "name": "nonce",
  "type": "int",
  "folder": True,
}, {
  "name": "bin",
  "type": "int",
  "folder": False,
}, {
  "name": "num_bins",
  "type": "int",
  "folder": True,
}, {
  "name": "file_id",
  "type": "int",
  "folder": False,
}, {
  "name": "execute",
  "type": "bool",
  "folder": False,
}, {
  "name": "num_files",
  "type": "int",
  "folder": False,
}, {
  "name": "suffix",
  "type": "alphanum",
  "folder": False,
}]

LOG_NAME = "/tmp/log.txt"


def check_output(command):
  try:
    stdout = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    return stdout
  except subprocess.CalledProcessError as e:
    print("ERROR", e.returncode, e.output)
    raise e


def is_set(params, key):
  if key not in params:
    return False
  return params[key]


def s3(params):
  [access_key, secret_key] = get_credentials(params["credential_profile"])
  session = boto3.Session(
           aws_access_key_id=access_key,
           aws_secret_access_key=secret_key,
           region_name=params["region"]
  )
  s3 = session.resource("s3")
  return s3


def object_exists(s3, bucket_name, key):
  try:
    s3.Object(bucket_name, key).load()
    return True
  except Exception:
    return False


def get_batch(bucket_name, key, prefix, params):
  objects = params["s3"].get_entries(bucket_name, prefix)
  batch_size = None if "batch_size" not in params else params["batch_size"]
  batch = []
  expected_batch_id = None
  if batch_size:
    expected_batch_id = int((parse_file_name(key)["file_id"] - 1) / batch_size)

  last = False
  for obj in objects:
    m = parse_file_name(obj.key)
    batch_id = int((m["file_id"] - 1) / batch_size) if batch_size else None
    if batch_size is None or batch_id == expected_batch_id:
      batch.append([obj, m])
      last = (m["num_files"] == m["file_id"])

  return [batch, last]


def combine_instance(bucket_name, key, params={}):
  done = False
  num_attempts = 20
  prefix = key_prefix(key) + "/"
  count = 0
  [batch, last] = get_batch(bucket_name, key, prefix, params)

  while not done and current_last_file(batch, key, params):
    [done, num_keys, num_files] = have_all_files(batch, prefix, params)
    count += 1
    if count == num_attempts and not done:
      return [False, None, False]
    if num_files is None:
      sleep = 5
    else:
      sleep = int((1 * num_files) / num_keys)
    time.sleep(sleep)
    [batch, last] = get_batch(bucket_name, key, prefix, params)

  keys = list(map(lambda b: b[0].key, batch))
  return [done and current_last_file(batch, key, params), keys, last]


def load_parameters(s3_dict, key_fields, start_time, event):
  # For cases where the lambda is triggered by a split / map function
  # wee need to look at the parameters for the prefix value. Otherwise
  # we just use the key prefix.
  if "extra_params" in s3_dict and "prefix" in s3_dict["extra_params"]:
    prefix = s3_dict["extra_params"]["prefix"]
  else:
    prefix = key_fields["prefix"]

  if is_set(event, "test"):
    params = event["load_func"]()
    s3 = event["s3"]
    client = event["client"]
  else:
    params = json.loads(open("{0:d}.json".format(prefix)).read())
    s3 = S3()
    client = boto3.client("lambda")

  params["offsets"] = []
  params["prefix"] = prefix
  params["s3"] = s3
  params["client"] = client

  if "extra_params" in s3_dict:
    params = {**params, **s3_dict["extra_params"]}
    for key in s3_dict["extra_params"].keys():
      params[key] = s3_dict["extra_params"][key]

  params["start_time"] = start_time
  params["payloads"] = []
  params["execute"] = is_set(event, "execute")

  return params


def handle(event, context, func):
  start_time = time.time()
  s3_dict = event["Records"][0]["s3"]
  bucket_name = s3_dict["bucket"]["name"]
  key = s3_dict["object"]["key"]
  input_format = parse_file_name(key)

  params = load_parameters(s3_dict, input_format, start_time, event)

  if run_function(params, input_format):
    [output_format, bucket_format] = get_formats(input_format, params)
    if not duplicate_execution(bucket_format, params):
      if not is_set(event, "test"):
        clear_tmp(params)
      make_folder(output_format)
      func(params["s3"], bucket_name, key, input_format, output_format, params["offsets"], params)

      show_duration(context, input_format, bucket_format, params)


def get_formats(input_format, params):
  output_format = dict(input_format)
  output_format["prefix"] = params["prefix"] + 1

  for key in ["file_id", "num_files"]:
    if key in params:
      output_format[key] = params[key]

  if params["file"] in ["combine_files", "split_file"]:
    bucket_format = dict(input_format)
  else:
    bucket_format = dict(output_format)

  bucket_format["ext"] = "log"
  bucket_format["prefix"] = output_format["prefix"]
  return [output_format, bucket_format]


def run_function(params, m):
  # The execute variable is for priority scheduling simulation.
  # We set it to false if we want to simulate the Lambda function
  # not executing due to a higher priority job.
  return is_set(params, "execute") or is_set(m, "execute")


def duplicate_execution(bucket_format, params):
  prefix = "-".join(file_name(bucket_format).split("-")[:-1])
  objects = params["s3"].get_entries(params["log"], prefix)
  return len(objects) != 0


def current_last_file(batch, current_key, params):
  entries: List[Entry] = list(map(lambda entry: entry[0], batch))
  entries = sorted(entries, key=lambda entry: [entry.last_modified_at(), entry.key])
  keys = set(list(map(lambda entry: entry.key, entries)))

  return ((current_key not in keys) or (entries[-1].key == current_key))


def have_all_files(batch, prefix, params):
  if "batch_size" in params:
    batch_id = int(batch[0][1]["file_id"] / params["batch_size"])
    max_batch_id = int(batch[0][1]["num_files"] / params["batch_size"])
  else:
    batch_id = 1
    max_batch_id = 1

  if batch_id < max_batch_id:
    num_files = params["batch_size"]
  else:
    if "batch_size" in params:
      num_files = ((batch[0][1]["num_files"] - 1) % params["batch_size"]) + 1
    else:
      num_files = batch[0][1]["num_files"]

  matching_keys = list(map(lambda b: b[0].key, batch))
  num_keys = len(matching_keys)
  return (num_keys == num_files, num_keys, num_files)


def lambda_setup(event, context):
  start_time = time.time()
  global FOUND
  FOUND = False
  if os.path.isfile("/tmp/warm"):
    FOUND = True

  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]
  key_fields = parse_file_name(key)
  if "extra_params" in s3 and "prefix" in s3["extra_params"]:
    prefix = s3["extra_params"]["prefix"]
  else:
    prefix = key_fields["prefix"]

  params = json.loads(open("{0:d}.json".format(prefix)).read())
  params["start_time"] = start_time
  params["payloads"] = []
  params["write_count"] = 0
  params["prefix"] = prefix
  params["token"] = random.randint(1, 100*1000*1000)
  params["key_fields"] = key_fields
  if is_set(event, "continue"):
    params["continue"] = True

  for value in ["object", "offsets", "pivots"]:
    if value in s3:
      params[value] = s3[value]

  if "extra_params" in s3:
    if "token" in s3["extra_params"]:
      params["parent_token"] = s3["extra_params"]["token"]
      s3["extra_params"]["token"] = params["token"]
    params = {**params, **s3["extra_params"]}

  return [bucket_name, key, params]


def show_duration(context, input_format, bucket_format, params):
  duration = params["timeout"] * 1000 - context.get_remaining_time_in_millis()

  log_results = {
    "payloads": params["s3"].payloads,
    "start_time": params["start_time"],
    "read_count": params["s3"].statistics.read_count,
    "write_count": params["s3"].statistics.write_count,
    "list_count": params["s3"].statistics.list_count,
    "write_byte_count": params["s3"].statistics.write_byte_count,
    "read_byte_count": params["s3"].statistics.read_byte_count,
    "duration": duration,
  }

  for key in ["name"]:
    log_results[key] = params[key]

  params["s3"].write(params["log"], file_name(bucket_format), str.encode(json.dumps(log_results)), {})


def print_request(m, params):
  if is_set(params, "test"):
    return

  msg = "{7:f} - TIMESTAMP {0:f} NONCE {1:d} STEP {2:d} BIN {3:d} FILE {4:d} TOKEN {5:d}"
  msg = msg.format(m["timestamp"], m["nonce"], params["prefix"], m["bin"], m["file_id"], params["token"], time.time())
  if "parent_token" in params:
    msg += " INVOKED BY TOKEN {0:d}".format(params["parent_token"])
  print(msg)
  msg += "\n"

  with open(LOG_NAME, "a+") as f:
    f.write(msg)


def print_read(m, key, params):
  print_action(m, key, "READ", params)


def print_write(m, key, params):
  print_action(m, key, "WRITE", params)


def print_action(m, key, action, params):
  if is_set(params, "test"):
    return

  msg = "{6:f} - TIMESTAMP {0:f} NONCE {1:d} STEP {2:d} BIN {3:d} {4:s} FILE NAME {5:s}"
  msg = msg.format(m["timestamp"], m["nonce"], params["prefix"], m["bin"], action, key, time.time())
  print(msg)
  msg += "\n"
  with open(LOG_NAME, "a+") as f:
    f.write(msg)


def setup_client(service, params):
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client(service,
                        region_name=params["region"],
                        config=config
                        )
  return client


def setup_connection(service, params):
  session = boto3.Session(
    aws_access_key_id=params["access_key"],
    aws_secret_access_key=params["secret_key"],
    region_name=params["region"]
  )
  return session.resource(service)


def key_prefix(key):
  return "/".join(key.split("/")[:-1])


def lambda_client(params):
  client = setup_client("lambda", params)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  #client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
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
  folder = False
  for part in FILE_FORMAT:
    if len(name) > 0:
      name += "/" if folder else "-"
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
      if part["type"] == "alphanum":
        name += "([A-Za-z0-9]+)"
      elif part["type"] == "float":
        name += "([0-9\.]+)"
      elif part["type"] == "int":
        name += "([0-9]+)"
      else:
        name += "([0-1])"
    folder = part["folder"]
  name += "."
  if "ext" in m:
    name += m["ext"]
  else:
    name += "([A-Za-z0-9]+)"

  return name


def make_folder(file_format):
  name = file_name(file_format)
  path = "/tmp/{0:s}".format(key_prefix(name))
  if not os.path.isdir(path):
    os.makedirs(path)


def file_name(m):
  if "execute" not in m:
    m["execute"] = True
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


def clear_tmp(params={}):
  if not is_set(params, "test"):
    subprocess.call("rm -rf /tmp/*", shell=True)

import boto3
import botocore
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

READ_BYTE_COUNT = 0
WRITE_BYTE_COUNT = 0
READ_COUNT = 0
LIST_COUNT = 0
WRITE_COUNT = 0
FOUND = False
DOWNLOAD_TIME = 0
LIST_TIME = 0
UPLOAD_TIME = 0


def invoke(client, name, params, payload):
  params["payloads"].append(payload)
  response = client.invoke(
    FunctionName=name,
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


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


def download(bucket, file):
  global READ_BYTE_COUNT
  global DOWNLOAD_TIME
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket)

  name = file.split("/")[-1]
  path = "/tmp/{0:s}".format(name)
  with open(path, "wb") as f:
    st = time.time()
    bucket.download_fileobj(file, f)
    et = time.time()
    DOWNLOAD_TIME += (et - st)
    READ_BYTE_COUNT += f.tell()
  return path


def get_objects(bucket_name, prefix=None, params={}):
  s3 = params["s3"]
  bucket = s3.Bucket(bucket_name)
  found = False
  while not found:
    try:
      if prefix is None:
        objects = bucket.objects.all()
      else:
        objects = bucket.objects.filter(Prefix=prefix)
      objects = list(objects)
      found = True
    except Exception as e:
      print("ERROR, util.get_objects", e)
      found = False
      time.sleep(1)

  return objects


def read(obj, start_byte, end_byte):
  global READ_COUNT
  READ_COUNT += 1
  global READ_BYTE_COUNT
  global DOWNLOAD_TIME
  READ_BYTE_COUNT += (end_byte - start_byte)
  st = time.time()
  content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read()
  et = time.time()
  DOWNLOAD_TIME += (et - st)
  return content.decode("utf-8")


def write(m, bucket, key, body, params):
  global UPLOAD_TIME
  global WRITE_BYTE_COUNT
  print_write(m, key, params)
  s3 = params["s3"] if "s3" in params else boto3.resource("s3")
  done = False
  while not done:
    try:
      st = time.time()
      s3.Object(bucket, key).put(Body=body, StorageClass=params["storage_class"])
      et = time.time()
      UPLOAD_TIME += (et - st)
      WRITE_BYTE_COUNT += s3.Object(bucket, key).content_length
      done = True
    except botocore.exceptions.ClientError as e:
      print("ERROR: RATE LIMIT")
      time.sleep(random.randint(1, 10))

  params["payloads"].append({
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket
        },
        "object": {
          "key": key
        }
      }
    }]
  })


def object_exists(bucket_name, key):
  try:
    s3 = boto3.resource("s3")
    s3.Object(bucket_name, key).load()
    return True
  except botocore.exceptions.ClientError as e:
    return False


def get_batch(bucket_name, key, prefix, params):
  objects = get_objects(bucket_name, prefix, params)
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
    s3 = boto3.resource("s3")
    client = boto3.client("lambda")

  params["offsets"] = {}
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
      func(bucket_name, key, input_format, output_format, params["offsets"], params)


def get_formats(input_format, params):
  output_format = dict(input_format)
  output_format["prefix"] = params["prefix"] + 1

  if "object" in params and "num_files" in params["object"]:
    for key in ["file_id", "num_files"]:
      output_format[key] = params["object"][key]

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
  objects = get_objects(params["log"], prefix, params)
  return len(objects) != 0


def current_last_file(batch, current_key, params):
  objects = list(map(lambda o: o[0], batch))
  objects = sorted(objects, key=lambda o: [o.last_modified, o.key])
  keys = set(list(map(lambda o: o.key, objects)))

  return ((current_key not in keys) or (objects[-1].key == current_key))


def have_all_files(batch, prefix, params):
  batch_id = int(batch[0][1]["file_id"] / params["batch_size"])
  max_batch_id = int(batch[0][1]["num_files"] / params["batch_size"])

  if batch_id < max_batch_id:
    num_files = params["batch_size"]
  else:
    num_files = ((batch[0][1]["num_files"] - 1) % params["batch_size"]) + 1

  matching_keys = list(map(lambda b: b[0].key, batch))
  num_keys = len(matching_keys)
  return (num_keys == num_files, num_keys, num_files)


def lambda_setup(event, context):
  start_time = time.time()
  global FOUND, READ_COUNT, READ_BYTE_COUNT, FOUND, LIST_COUNT
  READ_COUNT = 0
  LIST_COUNT = 0
  READ_BYTE_COUNT = 0
  FOUND = False
  global DOWNLOAD_TIME, LIST_TIME, UPLOAD_TIME, WRITE_BYTE_COUNT
  WRITE_BYTE_COUNT = 0
  DOWNLOAD_TIME = 0
  LIST_TIME = 0
  UPLOAD_TIME = 0
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


def show_duration(context, m, p):
  if m is None:
    return

  global READ_COUNT
  READ_COUNT += 1
  p["write_count"] += 1

  msg = "STEP {0:d} TOKEN {1:d} READ COUNT {2:d} WRITE COUNT {3:d} LIST COUNT {4:d} READ BYTE COUNT {5:d}\n"
  msg = msg.format(m["prefix"], p["token"], READ_COUNT, WRITE_COUNT, LIST_COUNT, READ_BYTE_COUNT)
  print(msg)
  duration = p["timeout"] * 1000 - context.get_remaining_time_in_millis()
  msg = "{8:f} - TIMESTAMP {0:f} NONCE {1:d} STEP {2:d} BIN {3:d} FILE {4:d} TOKEN {5:d} DURATION {6:d}"
  msg = msg.format(m["timestamp"], m["nonce"], p["prefix"], m["bin"], m["file_id"], p["token"], duration, time.time())
  print(msg)

  log_results = {
    "payloads": p["payloads"],
    "start_time": p["start_time"],
    "read_count": READ_COUNT,
    "write_count": p["write_count"],
    "list_count": LIST_COUNT,
    "write_byte_count": WRITE_BYTE_COUNT,
    "read_byte_count": READ_BYTE_COUNT,
    "duration": duration,
    "download_time": DOWNLOAD_TIME,
    "list_time": LIST_TIME,
    "upload_time": UPLOAD_TIME,
    "found": FOUND,
  }
  log_results = {**p, **m, **log_results}

  s3 = boto3.resource("s3")
  s3.Object(p["log"], file_name(p["bucket_format"])).put(Body=str.encode(json.dumps(log_results)))


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
                        aws_access_key_id=params["access_key"],
                        aws_secret_access_key=params["secret_key"],
                        region_name=params["region"],
                        config=config
                        )
  return client


def key_prefix(key):
  return "/".join(key.split("/")[:-1])


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

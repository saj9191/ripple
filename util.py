import boto3
import json
import os
import random
import re
import subprocess
import threading
import time
from database.s3 import S3
from botocore.client import Config


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
  "type": "float",
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


def combine_instance(bucket_name, key, params={}):
  # TODO: There's a race condition we need to figure out how to handle.
  # Consider the series of events
  # We have functions A and B. We want to combine the output of A and B
  # A_1 runs and outputs S_A
  # B_1 runs and outputs S_B
  # A_2 runs (before A_1 finishes) and outputs S_A

  # The combine phase triggered by the first S_A may see S_B and believe
  # it is not the last file and terminate.
  # The combine phase triggered by S_B may see the second S_A and 
  # believe it is not the last file and terminate.
  # The combine phase triggered by the second S_B may believe it is a
  # duplicate entry and terminate.

  # This will result in the file not being combined.
  # Our goal is to minimize time without introducing too much cost overhead.
  # There are a couple of solutions
  # 1. For combine look for duplicate entry AND look to see if combine actually happened.
  # -- This may require changes to how the functions are structured.
  # -- We need to know what the output format looks like. We want to make this generalizable
  # -- so we don't want to assume the next stage only has one file, etc.
  # -- This violates, exactly once semantics, but we don't have much control over that.
  # 2. For the function that performs the combine write all log files
  # -- I considered just not writing a log entry if the function did not result in a combine.
  # -- The problem with this is it will either complicate fault tolerance or in most cases
  # -- we will needlessly re-execute these functions when the fault tolerance notices there's
  # -- no log file. Both of these don't seem ideal. The fault tolerance shouldn't need to
  # -- understand complicated dependencies and re-executing everything is a lot of wasted work.
  # -- However, it would be fairly easy to change the combine logic to have one file write out
  # -- the log files. The problem with this is we need statistics for experiments. This would
  # -- be harder to gather, unless we write to different output files. One set of logs to just
  # -- gather stats and another set for fault tolerance, duplicate execution logic.
  # -- There could still be many duplicate executions.
  # 3. If a function sees all files are present, just combine. This may result in duplicate
  # -- work. There might still be a race condition where the last two functions finish at the
  # -- same time and don't see each other.
  # I think I like a mixture of #2 and #3. I think it would also give another incentive to
  # create folders for data execution, logs, and then statistics.
  # So the current plan is to to just combine if everything is present and have the combiner
  # write the logs. This should prevent overwrites from affecting combines and allow fault
  # tolerance to work.
  done = False
  num_attempts = 30
  m = parse_file_name(key)
  prefix = key_prefix(key) + "/"
  count = 0
  objects = params["database"].get_entries(bucket_name, prefix)
  last_file = current_last_file(objects, key)

  m["file_id"] = m["num_files"]
  last_file = file_name(m)

  while not done and last_file:
    done = have_all_files(objects, m)
    count += 1
    if count == num_attempts and not done:
      return [False, last_file, None]
    time.sleep(1)
    objects = params["database"].get_entries(bucket_name, prefix)
    last_file = current_last_file(objects, key)

  keys = list(map(lambda obj: obj.key, objects))
  return [done and last_file == key, last_file, keys]


def load_parameters(s3_dict, key_fields, start_time):
  # For cases where the lambda is triggered by a split / map function
  # wee need to look at the parameters for the prefix value. Otherwise
  # we just use the key prefix.
  if "extra_params" in s3_dict and "prefix" in s3_dict["extra_params"]:
    prefix = s3_dict["extra_params"]["prefix"]
  else:
    prefix = key_fields["prefix"]

  if is_set(s3_dict, "test"):
    params = s3_dict["load_func"]()
    params["test"] = True
  else:
    params = json.loads(open("{0:d}.json".format(prefix)).read())

  params["offsets"] = []
  params["prefix"] = prefix

  if "extra_params" in s3_dict:
    params = {**params, **s3_dict["extra_params"]}
    for key in s3_dict["extra_params"].keys():
      params[key] = s3_dict["extra_params"][key]

  params["start_time"] = start_time
  params["payloads"] = []
  if "execute" in s3_dict:
    params["reexecute"] = s3_dict["execute"]
  elif "extra_params" in s3_dict and "execute" in s3_dict["extra_params"]:
    params["reexecute"] = s3_dict["extra_params"]["execute"]

  #if random.randint(0, 100) < 10:
    #n = random.randint(60)
    #time.sleep(n)
    #print("Sleeping for", n, "seconds")
    #params["n"] = n

  if is_set(s3_dict, "test"):
    s3 = s3_dict["s3"]
    s3.params = params
  else:
    s3 = S3(params)

  if is_set(s3_dict, "reinvoke"):
    params["reinvoke"] = True

  if "ancestry" in s3_dict:
    params["ancestry"] = s3_dict["ancestry"]
  else:
    params["ancestry"] = []

  params["database"] = s3
  return params


def lambda_handle(event, context):
  s3_dict = event["Records"][0]["s3"]
  bucket_name = s3_dict["bucket"]["name"]
  key = s3_dict["object"]["key"]

  s3_dict = {**event, **s3_dict}
  return [bucket_name, key, s3_dict]


def openwhisk_handle(params):
  os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/action/aws/credentials"
  os.environ["AWS_CONFIG_FILE"] = "/action/aws/config"
  table = params["table"]
  key = params["key"]
  return [table, key, params]


def handle(argv, func):
  start_time = time.time()
  # TODO: Need a better way to handle this
  if len(argv) == 1:
    # OpenWhisk
    [table, key, params] = openwhisk_handle(argv[0])
  elif len(argv) == 2:
    # Lambda
    [table, key, params] = lambda_handle(argv[0], argv[1])
  else:
    raise Exception("Unexpected number of arguments", len(argv))
  input_format = parse_file_name(key)
  params = load_parameters(params, input_format, start_time)
  if run_function(params, input_format):
    [output_format, log_format] = get_formats(input_format, params)
    token = "{0:f}-{1:d}".format(log_format["timestamp"], log_format["nonce"])
    entry = prior_execution(log_format, params)
    params["ancestry"].append((token, log_format["prefix"], log_format["bin"], log_format["num_bins"], log_format["file_id"], log_format["num_files"]))
    if entry is None:
      if not is_set(params, "test"):
        clear_tmp(params)
      make_folder(output_format)
      try:
        finished = func(params["database"], table, key, input_format, output_format, params["offsets"], params)
        if finished:
          write_log(time.time() - start_time, input_format, log_format, params)
      except Exception as e:
        print("Exception", e)
        raise e
    else:
      count = 0
      content = json.loads(entry.get_content().decode("utf-8"))
      count += len(content["payloads"])
      for i in range(len(content["payloads"])):
        payload = content["payloads"][i]
        if "reexecute" in payload:
          payload["execute"] = params["reexecute"]
        params["database"].invoke(params["output_function"], payload)


def get_formats(input_format, params):
  output_format = dict(input_format)
  output_format["prefix"] = params["prefix"] + 1

  keys = set(list(map(lambda f: f["name"], FILE_FORMAT))).difference(set(["prefix"])).union(set(["ext"]))
  for key in keys:
    if key in params:
      output_format[key] = params[key]

  log_format = dict(output_format)
  log_format["prefix"] -= 1
  log_format["suffix"] = 0
  log_format["execute"] = 0
  log_format["ext"] = "log"

  return [output_format, log_format]


def run_function(params, m):
  if ("reexecute" in params and params["reexecute"] <= 0) or m["execute"] <= 0:
    return True
  now = time.time()
  return now < m["execute"]


def prior_execution(bucket_format, params):
  key = file_name(bucket_format)
  if params["database"].contains(params["log"], key):
    return params["database"].get_entry(params["log"], key)
  return None


def current_last_file(objects, current_key):
  objects = sorted(objects, key=lambda obj: [obj.last_modified_at(), obj.key])
  keys = list(map(lambda obj: obj.key, objects))
  return keys[-1]


def have_all_files(objects, m):
  num_files = m["num_files"]
  return num_files == len(objects)


def write_log(duration, input_format, bucket_format, params):
  duration = duration * 1000
  log_name = file_name(bucket_format)

  log_results = {**{
    "start_time": params["start_time"],
    "duration": duration,
    #"cpu": params["cpu"],
  },  **params["database"].get_statistics()}

  if "reinvoke" in params:
    log_results["invoke"] = True
  if "n" in params:
    log_results["n"] = params["n"]

  for key in ["name"]:
    log_results[key] = params[key]

  if not params["database"].contains(params["log"], file_name(bucket_format)):
    params["database"].write(params["log"], log_name, str.encode(json.dumps(log_results)), {}, invoke=False)


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
    m["execute"] = 0
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


class Monitor(threading.Thread):
  def __init__(self, cpu):
    super(Monitor, self).__init__()
    self.cpu = cpu
    self.running = True

  def run(self):
    while self.running:
      ps_results = subprocess.check_output("ps -o %cpu", shell=True).decode("utf-8").strip().split("\n")[1:]
      cpu = sum(map(lambda c: float(c), ps_results))
      self.cpu.append(cpu)
      time.sleep(5)

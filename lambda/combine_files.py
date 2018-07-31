import boto3
import importlib
import json
import util


def last_file(keys):
  last_modified_key = None
  last_modified_key_time = None
  keys.sort()
  for key in keys:
    m = util.parse_file_name(key)
    if last_modified_key_time is None or m["created"] > last_modified_key_time:
      last_modified_key_time = m["created"]
      last_modified_key = key
  return last_modified_key


def combine(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d} FILE {2:d}".format(ts, nonce, m["file-id"]))

  p = {
    "timestamp": ts,
    "nonce": nonce,
    "ext": m["ext"]
  }

  s3 = boto3.resource("s3")
  key_regex = util.get_key_regex(p)

  have_all_files = False
  keys = []
  while not have_all_files and (len(keys) == 0 or last_file(keys) == key):
    [have_all_files, keys] = util.have_all_files(bucket_name, key_regex)

  if have_all_files and last_file(keys) == key:
    print(ts, "Combining", len(keys))
    format_lib = importlib.import_module(params["format"])
    combine_function = getattr(format_lib, "combine")
    if params["sort"]:
      m["file-id"] = int(bucket_name.split("-")[-1])
      m["last"] = m["file-id"] == params["num_bins"]
    else:
      m["file-id"] = 1
      m["last"] = True
    file_name = util.file_name(m)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    combine_function(bucket_name, keys, temp_name, params)
    s3.Object(params["output_bucket"], file_name).put(Body=open(temp_name, "rb"))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  combine(bucket_name, key, params)

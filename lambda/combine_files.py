import boto3
import importlib
import json
import util


def last_file(s3, bucket, keys):
  last_modified_key = None
  last_modified_key_time = None
  keys.sort()
  for key in keys:
    obj = s3.Object(bucket, key)
    if last_modified_key_time is None or obj.last_modified > last_modified_key_time:
      last_modified_key = key
      last_modified_key_time = obj.last_modified
  return last_modified_key


def combine(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]

  p = {
    "timestamp": ts,
    "nonce": nonce,
    "ext": m["ext"]
  }

  s3 = boto3.resource("s3")
  key_regex = util.get_key_regex(p)
  have_all_files = False
  if m["last"]:
    while not have_all_files:
      [have_all_files, keys] = util.have_all_files(bucket_name, key_regex)

  if have_all_files:
    print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))
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
    content = combine_function(bucket_name, keys, temp_name, params)
    print("output_bucket", params["output_bucket"])
    s3.Object(params["output_bucket"], file_name).put(Body=str.encode(content))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  combine(bucket_name, key, params)

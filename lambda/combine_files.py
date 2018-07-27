import boto3
import importlib
import json
import util


def combine(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  num_bytes = m["max_id"]

  p = {
    "timestamp": ts,
    "nonce": nonce,
    "ext": m["ext"]
  }

  key_regex = util.get_key_regex(p)
  [have_all_files, keys] = util.have_all_files(bucket_name, num_bytes, key_regex)

  if have_all_files:
    print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))
    print(ts, "Combining", len(keys))
    s3 = boto3.resource("s3")
    format_lib = importlib.import_module(params["format"])
    combine_class = getattr(format_lib, "Combine")
    m["file-id"] = 1
    m["last"] = True
    file_name = util.file_name(m)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    combine_class.combine(bucket_name, keys, temp_name)
    s3.Object(params["output_bucket"], file_name).put(Body=open(temp_name, 'rb'))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  combine(bucket_name, key, params)

import boto3
import importlib
import util


def combine(bucket_name, key, params):
  util.clear_tmp()
  p = util.parse_file_name(key)
  util.print_request(p, params)
  util.print_read(p, key, params)
  m = dict(p)

  if params["sort"]:
    m["file_id"] = m["bin"]
    m["last"] = m["file_id"] == params["num_bins"]
  else:
    m["last"] = True
    m["file_id"] = 1
  m["bin"] = 1

  s3 = boto3.resource("s3")
  [combine, keys] = util.combine_instance(bucket_name, key)
  if combine:
    print("Combining TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d}".format(m["timestamp"], m["nonce"], m["bin"], m["file_id"]))
    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    m["prefix"] = params["prefix"] + 1
    file_name = util.file_name(m)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    iterator.combine(bucket_name, keys, temp_name, params)
    util.print_write(m, temp_name, params)
    s3.Object(params["bucket"], file_name).put(Body=open(temp_name, "rb"))
  return p


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = combine(bucket_name, key, params)
  util.show_duration(context, m, params)

import boto3
import importlib
import json
import util


def current_last_file(bucket_name, current_key):
  prefix = util.key_prefix(current_key)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  objects = list(bucket.objects.filter(Prefix=prefix))
  keys = set(list(map(lambda o: o.key, objects)))
  objects = sorted(objects, key=lambda o: [o.last_modified, o.key])
  return ((current_key not in keys) or (objects[-1].key == current_key))


def combine(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]

  if params["sort"]:
    m["file-id"] = int(bucket_name.split("-")[-1])
    m["last"] = m["file-id"] == params["num_bins"]

  util.print_request(m, params)
  util.print_read(m, key, params)

  p = {
    "timestamp": ts,
    "nonce": nonce,
    "ext": m["ext"]
  }

  s3 = boto3.resource("s3")
  key_regex = util.get_key_regex(p)

  have_all_files = False
  keys = []
  while not have_all_files and (len(keys) == 0 or current_last_file(bucket_name, key)):
    [have_all_files, keys] = util.have_all_files(bucket_name, key_regex)

  if have_all_files and current_last_file(bucket_name, key):
    print("Combining TIMESTAMP {0:f} NONCE {1:d} FILE {2:d}".format(ts, nonce, m["file-id"]))
    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    if not params["sort"]:
      m["file-id"] = 1
      m["last"] = True
    file_name = util.file_name(m)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    iterator.combine(bucket_name, keys, temp_name, params)
    util.print_write(m, file_name, params)
    s3.Object(params["output_bucket"], file_name).put(Body=open(temp_name, "rb"))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  params["request_id"] = context.aws_request_id
  combine(bucket_name, key, params)

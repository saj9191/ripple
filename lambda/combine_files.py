import boto3
import importlib
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
  p = util.parse_file_name(key)
  util.print_request(p, params)
  util.print_read(p, key, params)
  m = dict(p)

  if params["sort"]:
    m["file_id"] = m["bin"]
    m["last"] = m["file_id"] == params["num_bins"]
  else:
    m["last"] = True
    m["file-id"] = 1
  m["bin"] = 1

  s3 = boto3.resource("s3")
  have_all_files = False
  keys = []
  prefix = util.key_prefix(key)
  while not have_all_files and (len(keys) == 0 or current_last_file(bucket_name, key)):
    [have_all_files, keys] = util.have_all_files(bucket_name, prefix)

  if have_all_files and current_last_file(bucket_name, key):
    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    file_name = util.file_name(m)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    iterator.combine(bucket_name, keys, temp_name, params)
    util.print_write(m, file_name, params)
    s3.Object(params["output_bucket"], file_name).put(Body=open(temp_name, "rb"))
  return p


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = combine(bucket_name, key, params)
  util.show_duration(context, m, params)

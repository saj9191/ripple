import boto3
import importlib
import json
import util


def create_pivots(s3, sorted_input, params):
  max_identifier = int(sorted_input[-1][0] + 1)
  pivots = list(map(lambda p: p[0], sorted_input))
  num_bins = 2 * params["num_bins"] * params["num_bins"]
  increment = int(len(sorted_input) / num_bins)
  pivots = pivots[0::increment]
  if pivots[-1] == max_identifier - 1:
    pivots[-1] = max_identifier
  else:
    pivots.append(max_identifier)
  return pivots


def handle_pivots(bucket_name, key, m, start_byte, end_byte, params):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  sorted_input = iterator.get(obj, start_byte, end_byte, identifier=True)
  sorted_input = sorted(sorted_input, key=lambda k: k[0])
  pivots = create_pivots(s3, sorted_input, params)

  p = dict(m)
  p["name"] = "pivot"
  p["ext"] = "pivot"
  pivot_key = util.file_name(p)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  util.print_write(m, pivot_key, params)
  s3.Object(params["output_bucket"], pivot_key).put(Body=content)


def find_pivots(bucket_name, key, params, eparams):
  util.clear_tmp()
  m = util.parse_file_name(key)
  util.print_request(m, params)

  if "range" in eparams:
    rparams = eparams["range"]
    start_byte = rparams["start_byte"]
    end_byte = rparams["end_byte"]
    file_id = rparams["file_id"]
    more = rparams["more"]

    m["last"] = not more
    m["file-id"] = file_id
    handle_pivots(bucket_name, key, m, start_byte, end_byte, params)
  else:
    util.print_read(m, key, params)
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    handle_pivots(bucket_name, key, m, 0, obj.content_length, params)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  params["request_id"] = context.aws_request_id
  find_pivots(bucket_name, key, params, event["Records"][0]["s3"])

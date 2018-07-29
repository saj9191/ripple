import boto3
import importlib
import json
import util


def create_pivots(s3, sorted_input, params):
  min_identifier = int(sorted_input[0][0])
  max_identifier = int(sorted_input[-1][0] + 1)
  num_bins = params["num_bins"]
  increment = int((max_identifier - min_identifier + num_bins - 1) / num_bins)
  pivots = []
  pivot = min_identifier
  while pivot < max_identifier:
    pivots.append(pivot)
    pivot += increment
  pivots.append(pivot)

  return pivots


def find_pivots(bucket_name, key, file_id, start_byte, end_byte, more, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f} NONCE {1:d}".format(m["timestamp"], m["nonce"]))

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  sort_identifier = getattr(format_lib, "sortIdentifier")
  sorted_input = sort_identifier(obj, start_byte, end_byte)
  sorted_input = sorted(sorted_input, key=lambda k: k[0])

  m["last"] = not more
  m["file-id"] = file_id
  pivots = create_pivots(s3, sorted_input, params)

  p = dict(m)
  p["name"] = "pivot"
  p["ext"] = "pivot"
  pivot_key = util.file_name(p)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  s3.Object(params["output_bucket"], pivot_key).put(Body=content)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  start_byte = event["Records"][0]["s3"]["range"]["start_byte"]
  end_byte = event["Records"][0]["s3"]["range"]["end_byte"]
  file_id = event["Records"][0]["s3"]["range"]["file_id"]
  more = event["Records"][0]["s3"]["range"]["more"]
  find_pivots(bucket_name, key, file_id, start_byte, end_byte, more, params)

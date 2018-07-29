import boto3
import importlib
import json
import util


def bin_input(s3, sorted_input, format_lib, m, bin_ranges, params):
  bin_index = 0
  binned_input = list(map(lambda r: [], bin_ranges))
  for sinput in sorted_input:
    identifier = sinput[0]
    bin = None
    while bin is None:
      bin_range = bin_ranges[bin_index]["range"]
      if bin_range[0] <= identifier and identifier < bin_range[1]:
        bin = bin_index
      else:
        bin_index += 1

    binned_input[bin_index].append(sinput[1])

  iterator_class = getattr(format_lib, "Iterator")
  for i in range(len(binned_input)):
    content = iterator_class.from_array(binned_input[i])
    bin_key = util.file_name(m)
    s3.Object(bin_ranges[i]["bucket"], bin_key).put(Body=str.encode(content))


def sort(bucket_name, key, start_byte, end_byte, file_id, more, pivots, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f} NONCE {1:d}".format(m["timestamp"], m["nonce"]))

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  sort_identifier = getattr(format_lib, "sortIdentifier")
  print("sb", start_byte, "eb", end_byte)
  sorted_input = sort_identifier(obj, start_byte, end_byte)
  sorted_input = sorted(sorted_input, key=lambda k: k[0])
  print("sorted len", len(sorted_input))

  m["last"] = not more
  m["file-id"] = file_id
  bin_input(s3, sorted_input, format_lib, m, pivots, params)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  start_byte = event["Records"][0]["s3"]["range"]["start_byte"]
  end_byte = event["Records"][0]["s3"]["range"]["end_byte"]
  file_id = event["Records"][0]["s3"]["range"]["file_id"]
  more = event["Records"][0]["s3"]["range"]["more"]
  pivots = event["pivots"]
  sort(bucket_name, key, start_byte, end_byte, file_id, more, pivots, params)

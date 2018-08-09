import boto3
import importlib
import util


def bin_input(s3, sorted_input, format_lib, m, bin_ranges, params):
  bin_index = 0
  binned_input = list(map(lambda r: [], bin_ranges))
  count = 0
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
    count += 1

  count = 0
  iterator_class = getattr(format_lib, "Iterator")
  for i in range(len(binned_input)):
    count += len(binned_input[i])
    content = iterator_class.fromArray(binned_input[i])
    m["bin"] = bin_ranges[i]["bin"]
    bin_key = util.file_name(m)
    util.print_write(m, bin_key, params)
    s3.Object(bin_ranges[i]["bucket"], bin_key).put(Body=str.encode(content))


def sort(bucket_name, key, m, start_byte, end_byte, pivots, params):
  util.clear_tmp()
  util.print_request(m, params)
  if "extra_params" not in params or "token" not in params["extra_params"]:
    util.print_read(m, key, params)

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  sorted_input = iterator.get(obj, start_byte, end_byte, identifier=True)
  sorted_input = sorted(sorted_input, key=lambda k: k[0])

  bin_input(s3, sorted_input, format_lib, m, pivots, params)


def handle_sort(bucket_name, key, params, eparams, pivots):
  util.clear_tmp()
  m = util.parse_file_name(key)
  if "range" in eparams:
    rparams = eparams["range"]
    start_byte = rparams["start_byte"]
    end_byte = rparams["end_byte"]
    file_id = rparams["file_id"]
    more = rparams["more"]

    m["last"] = not more
    m["file_id"] = file_id
    sort(bucket_name, key, m, start_byte, end_byte, pivots, params)
  else:
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    sort(bucket_name, key, m, 0, obj.content_length, pivots, params)
  return m


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  s3 = event["Records"][0]["s3"]
  if "extra_params" in s3 and "target_bucket" in s3["extra_params"]:
    bucket_name = s3["extra_params"]["target_bucket"]
    key = s3["extra_params"]["target_file"]
  else:
    bucket_name = s3["bucket"]["name"]
    key = s3["object"]["key"]
  m = handle_sort(bucket_name, key, params, s3, event["pivots"])
  util.show_duration(context, m, params)

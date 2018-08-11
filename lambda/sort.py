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
    s3.Object(params["bucket"], bin_key).put(Body=str.encode(content))


def handle_sort(bucket_name, key, input_format, output_format, start_byte, end_byte, params):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  sorted_input = iterator.get(obj, start_byte, end_byte, params["identifier"])
  sorted_input = sorted(sorted_input, key=lambda k: k[0])

  bin_input(s3, sorted_input, format_lib, output_format, params["pivots"], params)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, handle_sort)
  util.show_duration(context, m, params)

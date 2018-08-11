import boto3
import importlib
import util


def find_match(bucket_name, key, input_format, output_format, start_byte, end_byte, params):
  [combine, keys] = util.combine_instance(bucket_name, key)
  if combine:
    best_match = None
    match_score = 0
    format_lib = importlib.import_module(params["format"])
    iterator = getattr(format_lib, "Iterator")
    s3 = boto3.resource("s3")

    for key in keys:
      obj = s3.Object(bucket_name, key)
      it = iterator(obj, params["batch_size"], params["chunk_size"])
      if params["find"] == "max sum":
        score = it.sum(params["identifier"])
      else:
        raise Exception("Not implemented", params["find"])

      if score > match_score:
        best_match = key
        match_score = score

    output_format["ext"] = "match"
    output_format["suffix"] = "match"
    file_name = util.file_name(output_format)
    util.print_write(output_format, file_name, params)
    s3.Object(bucket_name, file_name).put(Body=str.encode(best_match))


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, find_match)
  util.show_duration(context, m, params)

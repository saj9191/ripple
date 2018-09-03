import boto3
import importlib
import json
import pivot
import util


def split_file(bucket_name, key, input_format, output_format, offsets, params):
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  format_lib = importlib.import_module(params["format"])

  if util.is_set(params, "ranges"):
    [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key)
  else:
    input_bucket = bucket_name
    input_key = key

  obj = s3.Object(input_bucket, input_key)
  iterator_class = getattr(format_lib, "Iterator")
  iterator = iterator_class(obj, offsets, batch_size, chunk_size)

  more = True
  file_id = 0

  while more:
    file_id += 1
    [offsets, more] = iterator.nextOffsets()
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": input_bucket
          },
          "object": {
            "key": input_key,
            "file_id": file_id,
            "more": more
          },
          "offsets": offsets,
          "extra_params": {
            "token": params["token"],
            "prefix": output_format["prefix"]
          }
        }
      }]
    }

    if util.is_set(params, "ranges"):
      payload["Records"][0]["s3"]["extra_params"]["pivots"] = ranges

    response = client.invoke(
      FunctionName=params["output_function"],
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, split_file)
  util.show_duration(context, m, params)

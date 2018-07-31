import boto3
import importlib
import json
import pivot
import util


def split_file(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f} NONCE {1:d} FILE {2:d}".format(m["timestamp"], m["nonce"], m["file-id"]))

  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  format_lib = importlib.import_module(params["format"])

  more = True
  file_id = 0

  if "bucket_prefix" in params:
    [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key, params["bucket_prefix"])
  else:
    input_bucket = bucket_name
    input_key = key

  obj = s3.Object(input_bucket, input_key)
  iterator_class = getattr(format_lib, "Iterator")
  iterator = iterator_class(obj, batch_size, chunk_size)

  while more:
    file_id += 1
    [start_byte, end_byte, more] = iterator.nextOffsets()
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": input_bucket
          },
          "object": {
            "key": input_key
          },
          "range": {
            "file_id": file_id,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "more": more
          }
        }
      }]
    }
    if "bucket_prefix" in params:
      payload["pivots"] = ranges

    response = client.invoke(
      FunctionName=params["output_function"],
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  split_file(bucket_name, key, params)

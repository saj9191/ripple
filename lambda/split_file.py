import boto3
import importlib
import json
import util


def split_file(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ext = m["ext"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(m["timestamp"], m["nonce"]))

  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(ext)
  iterator_class = getattr(format_lib, "Iterator")
  iterator = iterator_class(obj, batch_size, chunk_size)

  more = True
  file_id = 0
  while more:
    file_id += 1
    [start_byte, end_byte, more] = iterator.nextOffsets()
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name
          },
          "object": {
            "key": key
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
    response = client.invoke(
      FunctionName="format_{0:s}_chunk".format(ext),
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("params.json").read())
  split_file(bucket_name, key, params)

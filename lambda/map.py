import boto3
import json
import pivot
import util


def map_file(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f} NONCE {1:d} FILE {2:d}".format(m["timestamp"], m["nonce"], m["file-id"]))

  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["map_bucket"])

  if "bucket_prefix" in params:
    [_, _, ranges] = pivot.get_pivot_ranges(bucket_name, key, params["bucket_prefix"])
    prefix = util.key_prefix(key)
    objects = bucket.objects.filter(Prefix=prefix)
  else:
    objects = bucket.objects.all()

  for obj in objects:
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name,
          },
          "object": {
            "key": key,
          },
          "extra_params": {
            "target_bucket": params["map_bucket"],
            "target_file": obj.key,
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
  s3 = event["Records"][0]["s3"]
  bucket_name = s3["bucket"]["name"]
  key = s3["object"]["key"]

  params = json.loads(open("params.json").read())
  map_file(bucket_name, key, params)

import boto3
import json
import util


def run(key, params, input_format, output_format):
  util.print_read(input_format, key, params)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["bucket"])
  objects = list(bucket.objects.filter(Prefix=params["input_key_prefix"] + "-"))
  assert(len(objects) == 1)
  obj_key = objects[0].key

  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["bucket"],
        },
        "object": {
          "key": key,
        },
        "extra_params": {
          "token": params["token"],
          "prefix": output_format["prefix"],
          "species": util.parse_file_name(obj_key)["suffix"]
        }
      }
    }]
  }

  client = boto3.client("lambda")
  response = client.invoke(
    FunctionName=params["output_function"],
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  return []

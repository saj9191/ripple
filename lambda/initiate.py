import boto3
import json
import util


def initiate(bucket_name, key, input_format, output_format, offsets, params):
  util.print_read(input_format, key, params)
  input_format["prefix"] = params["input_prefix"]
  prefix = util.key_prefix(util.file_name(input_format))
  objects = util.get_objects(params["bucket"], prefix, params)
  assert(len(objects) == 1)

  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["bucket"],
        },
        "object": {
          "key": objects[0].key,
        },
        "extra_params": {
          "prefix": output_format["prefix"],
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


def handler(event, context):
  util.handle(event, context, initiate)

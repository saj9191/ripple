import boto3
import json
import util


def initiate(bucket_name, key, input_format, output_format, offsets, params):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["bucket"])
  input_format["prefix"] = params["input_key_prefix"]
  prefix = util.key_prefix(util.file_name(input_format))
  objects = list(bucket.objects.filter(Prefix=prefix))
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
          "token": params["token"],
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
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, initiate)
  util.show_duration(context, m, params)

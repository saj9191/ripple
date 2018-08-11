import boto3
import json
import pivot
import util


def map_file(bucket_name, key, input_format, output_format, start_byte, end_byte, params):
  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["map_bucket"])

  if params["ranges"]:
    [_, _, ranges] = pivot.get_pivot_ranges(bucket_name, key)
    # TODO: Fix
    prefix = util.key_prefix(key)
    objects = bucket.objects.filter(Prefix=prefix)
  else:
    if "map_bucket_key_prefix" in params:
      objects = bucket.objects.filter(Prefix=params["map_bucket_key_prefix"] + "-")
    else:
      objects = bucket.objects.all()

  file_id = 0
  objects = list(filter(lambda obj: obj.key.endswith("/") == params["directories"], objects))

  for i in range(len(objects)):
    obj = objects[i]
    file_id += 1
    if params["directories"]:
      target_file = obj.key[:-1]
    else:
      target_file = obj.key

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name,
          },
          "object": {
          },
          "extra_params": {
            "token": params["token"],
            "target_bucket": params["map_bucket"],
            "target_file": target_file,
            "prefix": output_format["prefix"],
            "file_id": file_id,
            "more": (i + 1) != len(objects)
          }
        }
      }]
    }

    if params["input_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = key
      payload["Records"][0]["s3"]["extra_params"][params["bucket_key_value"]] = target_file
    elif params["bucket_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = target_file
      payload["Records"][0]["s3"]["extra_params"][params["input_key_value"]] = key
    else:
      raise Exception("Need to specify field for map key")

    if params["ranges"]:
      payload["pivots"] = ranges

    response = client.invoke(
      FunctionName=params["output_function"],
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, map_file)
  util.show_duration(context, m, params)

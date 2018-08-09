import boto3
import json
import pivot
import util


def map_file(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  util.print_request(m, params)

  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["map_bucket"])

  if params["ranges"]:
    [_, _, ranges] = pivot.get_pivot_ranges(bucket_name, key, params["bucket_prefix"], params["num_buckets"])
    # TODO: Fix
    prefix = util.key_prefix(key)
    objects = bucket.objects.filter(Prefix=prefix)
  else:
    objects = bucket.objects.all()

  for obj in objects:
    if not (params["directories"] == obj.key.endswith("/")):
      continue

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
            "token": params["token"],
            "target_bucket": params["map_bucket"],
            "target_file": obj.key,
            "prefix": params["key_fields"]["prefix"] + 1,
          }
        }
      }]
    }

    if params["ranges"]:
      payload["pivots"] = ranges

    response = client.invoke(
      FunctionName=params["output_function"],
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  return m


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = map_file(bucket_name, key, params)
  util.show_duration(context, m, params)

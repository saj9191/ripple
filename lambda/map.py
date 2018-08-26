import boto3
import json
import pivot
import util


def map_file(bucket_name, key, input_format, output_format, offsets, params):
  client = boto3.client("lambda")
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(params["map_bucket"])
  util.print_read(input_format, key, params)

  if params["ranges"]:
    print("pivot", "bucket", bucket_name, "key", key)
    [bucket_name, key, ranges] = pivot.get_pivot_ranges(bucket_name, key)
    prefix = util.key_prefix(key)
    bucket = s3.Bucket(bucket_name)
    objects = list(bucket.objects.filter(Prefix=prefix))
    objects = list(set(map(lambda o: o.key, objects)))
  else:
    if "map_bucket_key_prefix" in params:
      objects = list(bucket.objects.filter(Prefix=params["map_bucket_key_prefix"] + "-"))
      objects = list(set(map(lambda o: o.key, objects)))
    else:
      objects = list(bucket.objects.all())
      if params["directories"]:
        objects = list(filter(lambda o: "/" in o.key, objects))
        objects = list(set(map(lambda o: o.key.split("/")[0], objects)))
      else:
        objects = list(set(map(lambda o: o.key, objects)))

  file_id = 0
  print("num objects", len(objects))

  for i in range(len(objects)):
    obj = objects[i]
    file_id += 1
    target_file = obj

    print("target", target_file)

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name,
          },
          "object": {
            "file_id": file_id,
            "more": (i + 1) != len(objects)
          },
          "extra_params": {
            "token": params["token"],
            "target_bucket": params["map_bucket"],
            "target_file": target_file,
            "prefix": output_format["prefix"],
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
      payload["Records"][0]["s3"]["extra_params"]["pivots"] = ranges

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

import boto3
import json
import threading
import time

bucket_name = "maccoss-tide-east-1"

def invoke(obj):
  s3 = boto3.resource("s3")
  client = boto3.client("lambda", region_name="us-east-1")
  bucket = s3.Bucket(bucket_name)
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket_name
        },
        "object": {
          "key": obj.key
        }
      }
    }]
  }
  client.invoke(
    FunctionName="split_file",
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )

s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
objs = list(bucket.objects.filter(Prefix="0/"))[:100]
threads = []
for obj in objs:
  threads.append(threading.Thread(target=invoke, args=(obj,)) )
  threads[-1].start()

for thread in threads:
  thread.join()

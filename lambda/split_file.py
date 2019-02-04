import boto3
import pivot
import util
from typing import Any, Dict, List, Optional


def create_payload(bucket: str, key: str, offsets: List[int], prefix: int, file_id: Optional[int]=None, num_files: Optional[int]=None):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket
        },
        "object": {
          "key": key,
        },
        "extra_params": {
          "prefix": prefix,
          "offsets": offsets,
        }
      }
    }]
  }
  if file_id is not None:
    payload["Records"][0]["s3"]["extra_params"]["file_id"] = file_id
  if num_files is not None:
    payload["Records"][0]["s3"]["extra_params"]["num_files"] = num_files

  return payload


def split_file(bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  split_size = params["split_size"]

  s3 = params["s3"] if "s3" in params else boto3.resource("s3")
  client = params["client"] if "client" in params else boto3.client("lambda")

  if util.is_set(params, "ranges"):
    [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key)
  else:
    input_bucket = bucket_name
    input_key = key

  obj = s3.Object(input_bucket, input_key)

  file_id = params["file_id"] if "file_id" in params else 1

  num_files = int((obj.content_length + split_size - 1) / split_size)

  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(obj.content_length, (file_id) * split_size) - 1]
    payload = create_payload(input_bucket, input_key, offsets, output_format["prefix"], file_id, num_files)

    s3_params = payload["Records"][0]["s3"]

    if util.is_set(params, "ranges"):
      s3_params["extra_params"]["pivots"] = ranges

    util.invoke(client, params["output_function"], params, payload)
    file_id += 1


def handler(event, context):
  util.handle(event, context, split_file)

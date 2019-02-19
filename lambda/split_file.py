import boto3
import pivot
import util
from database import Database
from typing import Any, Dict, List, Optional


def create_payload(bucket: str, key: str, offsets: List[int], prefix: int, input_format, file_id: Optional[int]=None, num_files: Optional[int]=None):
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
          "bin": input_format["bin"],
          "num_bins": input_format["num_bins"],
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


def split_file(d: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  split_size = params["split_size"]

  client = params["client"] if "client" in params else boto3.client("lambda")

  if util.is_set(params, "ranges"):
    [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key, params)
  else:
    input_bucket = bucket_name
    input_key = key

  obj = d.get_entry(input_bucket, input_key)

  file_id = params["file_id"] if "file_id" in params else 1

  content_length: int = obj.content_length()
  num_files = int((content_length + split_size - 1) / split_size)

  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(content_length, (file_id) * split_size) - 1]
    payload = create_payload(input_bucket, input_key, offsets, output_format["prefix"], input_format, file_id, num_files)

    s3_params = payload["Records"][0]["s3"]

    if util.is_set(params, "ranges"):
      s3_params["extra_params"]["pivots"] = ranges

    d.invoke(client, params["output_function"], params, payload)
    file_id += 1


def handler(event, context):
  util.handle(event, context, split_file)

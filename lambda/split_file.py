import boto3
import pivot
import util
from database import Database
from typing import Any, Dict, List, Optional


def create_payload(bucket: str, key: str, offsets: List[int], output_format, file_id: Optional[int]=None, num_files: Optional[int]=None):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket
        },
        "object": {
          "key": key,
        },
        "extra_params": {**output_format, **{"offsets": offsets}}
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

  input_bucket = bucket_name
  if util.is_set(params, "ranges"):
    if "input_prefix" in params:
      obj = d.get_entries(bucket_name, params["input_prefix"])[0]
      input_key = obj.key
      pivot_key = d.get_entries(bucket_name, "4/")[0].key # TODO Unhardcode
      [_, _, ranges] = pivot.get_pivot_ranges(bucket_name, pivot_key, params)
    else:
      [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key, params)
      obj = d.get_entry(input_bucket, input_key)
  else:
    input_key = key
    obj = d.get_entry(input_bucket, input_key)

  output_format["ext"] = obj.key.split(".")[-1]
  assert("ext" not in output_format or output_format["ext"] != "pivot")
  file_id = 1 
  content_length: int = obj.content_length()
  num_files = int((content_length + split_size - 1) / split_size)

  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(content_length, (file_id) * split_size) - 1]
    payload = create_payload(input_bucket, input_key, offsets, output_format, file_id, num_files)

    s3_params = payload["Records"][0]["s3"]

    if util.is_set(params, "ranges"):
      s3_params["extra_params"]["pivots"] = ranges

    d.invoke(params["output_function"], payload)
    file_id += 1


def handler(event, context):
  util.handle(event, context, split_file)

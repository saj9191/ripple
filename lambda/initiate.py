import boto3
import util
from database import Database
from typing import Any, Dict, List


def initiate(d: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["trigger_bucket"],
        },
        "object": {
          "key": params["trigger_key"],
        },
        "extra_params": {
          "prefix": output_format["prefix"],
        }
      }
    }]
  }

  client = boto3.client("lambda")
  d.invoke(client, params["output_function"], params, payload)


def handler(event, context):
  util.handle(event, context, initiate)

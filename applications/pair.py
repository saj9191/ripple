import util
from database import Database
from typing import List


def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  train_key = "train.classification.w1-h1"
  obj = database.get_entry("maccoss-spacenet", train_key)
  client = params["client"] if "client" in params else boto3.client("lambda")
  content_length: int = obj.content_length()
  split_size = params["split_size"]
  num_files = int((content_length + split_size - 1) / split_size)
  file_id = 1

  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(content_length, (file_id) * split_size) - 1]

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": params["bucket"],
          },
          "object": {
            "key": util.file_name(input_format),
          },
          "extra_params": {
            "file_id": file_id,
            "num_files": num_files,
            "bin": output_format["bin"],
            "num_bins": output_format["num_bins"],
            "train_key": train_key,
            "prefix": output_format["prefix"],
            "train_offsets": offsets,
          }
        }
      }],
      "log": [output_format["prefix"], output_format["bin"], file_id]
    }

    database.invoke(client, params["output_function"], params, payload)
    file_id += 1

  return []

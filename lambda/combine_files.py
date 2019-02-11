import importlib
import os
import util
from database import Database, Entry
from typing import Any, Dict, List


def combine(database: Database, bucket_name, key, input_format, output_format, offsets, params):
  batch_size = params["batch_size"] if "batch_size" in params else 1

  # TODO: Fix.
  if "batch_size" in params:
    output_format["file_id"] = int((input_format["file_id"] + batch_size - 1) / batch_size)
  else:
    output_format["file_id"] = input_format["bin"]
  output_format["bin"] = 1
  output_format["num_bins"] = 1
  util.make_folder(output_format)

  [combine, keys, last] = util.combine_instance(bucket_name, key, params)
  if combine:
    if "batch_size" in params:
      output_format["num_files"] = int((input_format["num_files"] + params["batch_size"] - 1) / params["batch_size"])
    else:
      output_format["num_files"] = input_format["num_bins"]
    msg = "Combining TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d}"
    msg = msg.format(input_format["timestamp"], input_format["nonce"], input_format["bin"], input_format["file_id"])
    print(msg)

    format_lib = importlib.import_module(params["format"])
    iterator_class = getattr(format_lib, "Iterator")
    file_name = util.file_name(output_format)
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    entries: List[Entry] = list(map(lambda key: database.get_entry(bucket_name, key), keys))
    metadata: Dict[str, str] = {}
    with open(temp_name, "wb+") as f:
      metadata = iterator_class.combine(entries, f, params)

    with open(temp_name, "rb") as f:
      params["s3"].put(params["bucket"], file_name, f, metadata)
    os.remove(temp_name)


def handler(event, context):
  util.handle(event, context, combine)

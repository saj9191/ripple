import util
from database import Database
import threading
from typing import List


def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  train_key = "train.classification.w1-h1"
  obj = database.get_entry("maccoss-spacenet", train_key)
  content_length: int = obj.content_length()
  split_size = params["split_size"]
  num_files = int((content_length + split_size - 1) / split_size)
  file_id = 1

  threads = []
  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(content_length, (file_id) * split_size) - 1]
    extra_params = {**output_format, **{
      "file_id": file_id,
      "num_files": num_files,
      "train_key": train_key,
      "train_offsets": offsets,
    }}
    payload = database.create_payload(params["bucket"], util.file_name(input_format), extra_params)
    payload["log"] = [output_format["prefix"], output_format["bin"], file_id]

    threads.append(threading.Thread(target=database.invoke, args=(params["output_function"], payload)))
    threads[-1].start()
    file_id += 1

  for thread in threads:
    thread.join()
  return []

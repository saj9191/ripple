# This file is part of Ripple.

# Ripple is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Ripple is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Ripple.  If not, see <https://www.gnu.org/licenses/>.

import util
from database.database import Database, Entry
from typing import Any, Dict, List


def map_file(database: Database, table: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  prefix: str = util.key_prefix(key)

  if "map_bucket_key_prefix" in params:
    items: List[Entry] = database.get_entries(params["map_bucket"], prefix=params["map_bucket_key_prefix"])
    keys: List[str] = list(set(map(lambda item: item.key, items)))
  else:
    items: List[Entry] = database.get_entries(params["map_bucket"])
    keys: List[str] = list(set(map(lambda item: item.key, items)))

  file_id = 0
  num_files = len(keys)
  keys.sort()
  for i in range(num_files):
    target_file = keys[i]
    file_id += 1

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": table,
          },
          "object": {
          },
          "extra_params": {
            "target_bucket": params["map_bucket"],
            "target_file": target_file,
            "prefix": output_format["prefix"],
            "file_id": file_id,
            "num_files": num_files,
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

    database.invoke(params["output_function"], payload)


def main(*argv):
  util.handle(argv, map_file)

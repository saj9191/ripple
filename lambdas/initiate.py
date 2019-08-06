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
from database import Database
from typing import Any, Dict, List


def initiate(database: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  [combine, keys, last] = util.combine_instance(bucket_name, key, params)
  if "trigger_key" in params:
    bucket = params["trigger_bucket"]
    key = params["trigger_key"]
  else:
    bucket = bucket_name
    key = database.get_entries(bucket_name, params["input_prefix"])[0].key 

  if combine:
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket
          },
          "object": {
            "key": key
          },
          "extra_params": output_format
        }
      }]
    }
    database.invoke(params["output_function"], payload)


def main(*argv):
  util.handle(argv, initiate)

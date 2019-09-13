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

import importlib
import util
from database.database import Database
from typing import Any, Dict, List, Tuple

def find_match(database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  [combine, last, keys] = util.combine_instance(bucket_name, key, params)
  if combine:
    print("Finding match")
    best_match = None
    match_score = 0
    format_lib = importlib.import_module("formats." + params["input_format"])
    iterator_class = getattr(format_lib, "Iterator")

    keys.sort()
    with open(util.LOG_NAME, "a+") as f:
      for key in keys:
        entry = database.get_entry(bucket_name, key)
        it = iterator_class(entry, None)
        score: float = it.sum(format_lib.Identifiers[params["identifier"]])

        print("key {0:s} score {1:d}".format(key, score))
        f.write("key {0:s} score {1:d}\n".format(key, score))
        if score > match_score:
          best_match = key
          match_score = score

    if best_match is None:
      best_match = keys[0]

    output_format["ext"] = "match"
    file_name = util.file_name(output_format)
    database.write(bucket_name, file_name, str.encode(best_match), {}, True)


def main(*argv):
  util.handle(argv, find_match)

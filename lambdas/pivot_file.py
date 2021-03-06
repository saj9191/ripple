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

import boto3
import importlib
import util
from database.database import Database, Entry
from formats.iterator import OffsetBounds
from typing import Any, Dict, List


def create_pivots(d: Database, format_lib: Any, iterator_class: Any, items: List[Any], params: Dict[str, Any]) -> List[float]:
  if len(items) == 0:
    return []

  pivots: List[float] = map(lambda item: iterator_class.get_identifier_value(item, format_lib.Identifiers[params["identifier"]]), items)
  pivots = list(set(list(pivots)))
  pivots.sort()

  max_identifier: float = float(pivots[-1] + 1)
  # TODO: Competition between parameters and key parameters. Need to fix
  num_bins = params["num_pivot_bins"]
  increment = int(len(items) / num_bins)
  pivots = pivots[0::increment]
  if pivots[-1] == max_identifier - 1:
    pivots[-1] = max_identifier
  else:
    pivots.append(max_identifier)
  return pivots


def handle_pivots(database: Database, bucket_name, key, input_format, output_format, offsets, params):
  entry: Entry = database.get_entry(bucket_name, key)

  format_lib = importlib.import_module("formats." + params["input_format"])
  iterator_class = getattr(format_lib, "Iterator")
  if len(offsets) > 0:
    it = iterator_class(entry, OffsetBounds(offsets[0], offsets[1]))
  else:
    it = iterator_class(entry, None)

  items = it.get(it.get_start_index(), it.get_end_index())
  pivots: List[float] = create_pivots(database, format_lib, iterator_class, list(items), params)

  output_format["ext"] = "pivot"
  pivot_key = util.file_name(output_format)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  database.write(params["bucket"], pivot_key, content, {}, True)
  return True


def main(*argv):
  util.handle(argv, handle_pivots)

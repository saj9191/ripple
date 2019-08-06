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
from formats.iterator import OffsetBounds
from typing import Any, Dict, List, Tuple


def bin_input(sorted_input: List[Tuple[float, Any]], bin_ranges: List[Dict[str, int]]) -> List[Any]:
  bin_index = 0
  binned_input = list(map(lambda r: [], bin_ranges))
  count = 0
  for sinput in sorted_input:
    identifier = sinput[0]
    bin = None
    while bin is None:
      bin_range = bin_ranges[bin_index]["range"]
      if bin_range[0] <= identifier and identifier < bin_range[1]:
        bin = bin_index
      else:
        bin_index += 1
    binned_input[bin_index].append(sinput[1])
    count += 1
  return binned_input


def write_binned_input(database: Database, binned_input: List[Any], bin_ranges: List[Dict[str, int]], extra: Dict[str, Any], output_format, iterator_class, params):
  for i in range(len(binned_input)):
    [content, metadata] = iterator_class.from_array(binned_input[i], None, extra)
    output_format["bin"] = bin_ranges[i]["bin"]
    output_format["num_bins"] = len(bin_ranges)
    bin_key = util.file_name(output_format)
    database.write(params["bucket"], bin_key, content, metadata, True)


def handle_sort(database: Database, table_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  entry = database.get_entry(table_name, key)
  assert("ext" in output_format)
  format_lib = importlib.import_module("formats." + params["input_format"])
  iterator_class = getattr(format_lib, "Iterator")
  if len(offsets) > 0:
    it = iterator_class(entry, OffsetBounds(offsets[0], offsets[1]))
  else:
    it = iterator_class(entry, None)
  extra = it.get_extra()
  items = it.get(it.get_start_index(), it.get_end_index())
  items = list(map(lambda item: (it.get_identifier_value(item, format_lib.Identifiers[params["identifier"]]), item), items))
  sorted_items = sorted(items, key=lambda k: k[0])
  bin_ranges = params["pivots"]
  binned_input = bin_input(sorted_items, bin_ranges)
  write_binned_input(database, binned_input, bin_ranges, extra, dict(output_format), iterator_class, params)
  return True


def main(*argv):
  util.handle(argv, handle_sort)

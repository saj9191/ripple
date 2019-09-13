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

import heapq
import importlib
import util
from database.database import Database
from typing import Any, Dict, List


class Element:
  def __init__(self, identifier, value):
    self.value = value
    self.identifier = identifier

  def __lt__(self, other):
    return [self.identifier, self.value] < [other.identifier, other.value]


def find_top(d: Database, table: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  entry = d.get_entry(table, key)
  format_lib = importlib.import_module("formats." + params["input_format"])
  iterator_class = getattr(format_lib, "Iterator")
  if len(offsets) > 0:
    it = iterator_class(entry, OffsetBounds(offsets[0], offsets[1]))
  else:
    it = iterator_class(entry, None)

  top = []
  more = True
  while more:
    [items, _, more] = it.next()

    for item in items:
      score: float = it.get_identifier_value(item, params["identifier"])
      heapq.heappush(top, Element(score, item))
      if len(top) > params["number"]:
        heapq.heappop(top)

  file_name = util.file_name(output_format)
  temp_name = "/tmp/{0:s}".format(file_name)
  items = list(map(lambda t: t.value, top))
  with open(temp_name, "wb+") as f:
    [content, metadata] = iterator_class.from_array(items, f, it.get_extra())

  with open(temp_name, "rb") as f:
    d.put(table, file_name, f, metadata)


def main(*argv):
  util.handle(argv, find_top)

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
import util
from formats import iterator
from database.database import Entry
from formats.iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, BinaryIO, ClassVar, Dict, List, Optional


class Iterator(iterator.Iterator[None]):
  delimiter: Delimiter = Delimiter(item_token="\n\n", offset_token="\n\n", position=DelimiterPosition.inbetween)
  identifiers: None
  increment: ClassVar[int] = 25  # TODO: Unhardcode

  def __init__(self, entry: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, entry, offset_bounds)

  @classmethod
  def combine(cls: Any, entries: List[Entry], f: BinaryIO, extra: Dict[str, Any]) -> Dict[str, str]:
    pivots = set()
    file_key: Optional[str] = None
    for entry in entries:
      content: str = entry.get_content().decode("utf-8")
      [file_bucket, file_key, pivot_content] = content.split("\n")
      pivot_content: str = pivot_content.strip()
      if len(pivot_content) > 0:
        new_pivots = set(list(map(lambda p: float(p), pivot_content.split("\t"))))
        pivots = pivots.union(new_pivots)
    assert(file_key is not None)

    pivots = list(pivots)
    pivots.sort()
    super_pivots: List[int] = []
    num_bins = extra["num_pivot_bins"] # TODO: Figure out what to do if we want to hardcode this
    #num_bins = int((len(pivots) + cls.increment) / cls.increment)
    increment = float(len(pivots)) / num_bins

    i: int = 0
    while i < len(pivots):
      x: int = min(round(i), len(pivots) - 1)
      super_pivots.append(pivots[x])
      i += increment

    if super_pivots[-1] != pivots[-1]:
      super_pivots.append(pivots[-1])
    spivots: List[str] = list(map(lambda p: str(p), super_pivots))
    content: str = "{0:s}\n{1:s}\n{2:s}".format(file_bucket, file_key, "\t".join(spivots))
    f.write(str.encode(content))
    return {}


def get_pivot_ranges(bucket_name, key, params={}):
  ranges = []

  content: str = params["database"].get_entry(bucket_name, key).get_content().decode("utf-8")
  [file_bucket, file_key, pivot_content] = content.split("\n")
  pivots = list(map(lambda p: float(p), pivot_content.split("\t")))

  for i in range(len(pivots) - 1):
    end_range = int(pivots[i + 1])
    ranges.append({
      "range": [int(pivots[i]), end_range],
      "bin": i + 1,
    })

  return file_bucket, file_key, ranges

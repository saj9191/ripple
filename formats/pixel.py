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

import iterator
import new_line
from database import Entry
from iterator import OffsetBounds, Optional
from typing import Any, BinaryIO, Dict, Iterable, List, Tuple


# PIXEL ITERATOR
# Expects files to put of the format
# x1 y1 r1 g1 b1
# x2 y2 r2 g2 b2
#         *
#         *
#         *
# xn yn rn gn bn


Pixel = Tuple[int, int, int, int, int]


def __to_pixel__(item: str) -> Pixel:
  parts: List[int] = list(map(lambda i: int(i), item.split(" ")))
  assert(len(parts) == 5)
  return (parts[0], parts[1], parts[2], parts[3], parts[4])


class Iterator(new_line.Iterator):
  identifiers = None

  def __init__(self, obj: Entry, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def from_array(cls: Any, items: List[Pixel], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    content: str = cls.Iterator.delimiter.item_token.join(list(map(lambda i: " ".join(i), items)))
    if f:
      f.write(str.encode(content))
    return (content, {})

  @classmethod
  def to_array(cls: Any, content: str) -> Iterable[Pixel]:
    items = filter(lambda item: len(item.strip()) > 0, content.split(cls.delimiter.item_token))
    return map(lambda item: __to_pixel__(item), items)

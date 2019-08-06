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

from formats import new_line
from formats.iterator import OffsetBounds, Options
from typing import Any, ClassVar, Generic, List, Optional, TypeVar


T = TypeVar("T")


class Iterator(Generic[T], new_line.Iterator[T]):
  identifiers: T
  item_delimiter: ClassVar[str] = "\t"
  options: ClassVar[Options] = Options(has_header = True)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    new_line.Iterator.__init__(self, obj, offset_bounds)

  @classmethod
  def to_tsv_array(cls: Any, items: List[str]) -> List[List[str]]:
    tsv_items: List[List[str]] = list(map(lambda item: item.split(cls.item_delimiter), items))
    return tsv_items

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: T) -> float:
    raise Exception("Not Implemented")

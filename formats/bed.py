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

import new_line
from enum import Enum
from iterator import OffsetBounds, Options
from typing import Any, ClassVar, Generic, List, Optional, TypeVar


T = TypeVar("T")


class Identifiers(Enum):
  start_position = 0
  end_position = 1


class Iterator(new_line.Iterator):
  identifiers: Identifiers

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    new_line.Iterator.__init__(self, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: T) -> float:
    parts = item.split(b"\t")
    return float(parts[identifier.value + 1])


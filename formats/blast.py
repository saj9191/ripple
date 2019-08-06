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

import re
import util
from enum import Enum
from formats import iterator
from formats.iterator import Delimiter, DelimiterPosition, OffsetBounds, Optional, Options
from typing import Any, ClassVar, Generic


class Identifiers(Enum):
  score = 0


class Iterator(iterator.Iterator[Identifiers]):
  delimiter: Delimiter = Delimiter(item_token="\n\n", offset_token="\n\n", position=DelimiterPosition.inbetween)
  identifiers: Identifiers
  optimal_score_regex = re.compile(r"[\S\s]*^optimal_alignment_score:\s([0-9]+)[\S\s]*", re.MULTILINE)
  options: ClassVar[Options] = Options(has_header=False)
  suboptimal_score_regex = re.compile(r"[\S\s]*suboptimal_alignment_score:\s([0-9]+)[\S\s]*", re.MULTILINE)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: str) -> float:
    s: str = item.decode("utf-8")
    m = cls.optimal_score_regex.match(s)
    score = float(m.group(1)) * 1000
    m = Iterator.suboptimal_score_regex.match(s)
    if m is not None:
      score += float(m.group(1))
    return score

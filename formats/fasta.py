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
from formats.iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, ClassVar, Optional


class Iterator(iterator.Iterator[None]):
  delimiter: Delimiter = Delimiter(item_token=">", offset_token=">", position=DelimiterPosition.start)
  options: ClassVar[Options] = Options(has_header = False)
  identifiers = None

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

import boto3
import iterator
import util
from iterator import Delimiter, DelimiterPosition, OffsetBounds
from typing import Any, Optional


class Iterator(iterator.Iterator):
  delimiter: Delimiter = Delimiter("\n", DelimiterPosition.inbetween)
  identifiers: None = None

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

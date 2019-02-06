import iterator
import re
import util
from enum import Enum
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Optional
from typing import Any, ClassVar


class Identifiers(Enum):
  signature = 0


class Iterator(iterator.Iterator[Identifiers]):
  delimiter: Delimiter = Delimiter(item_token="@cluster", offset_token="@cluster", position=DelimiterPosition.start)
  identifiers: Identifiers
  signature_length: ClassVar[int] = 8

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: Identifiers) -> float:
    lines = item.split("\n")
    print(lines)

    seq = lines[1]
    print("seq is",seq)
    identifier = seq[0:7]
    
    for i in range(len(seq)-cls.signature_length+1):
      window = seq[i:i+7]
      if window < identifier:
        identifier = window
    print("identifier is",identifier)
    
    identifier_value = 0
    for i in range(cls.signature_length):
      identifier_value *= 10
      identifier_value += ord(seq[i])

    return identifier_value


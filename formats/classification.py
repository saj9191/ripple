import iterator
import new_line
from database import Entry
from iterator import OffsetBounds, Optional
from typing import Any, BinaryIO, Dict, Iterable, List, Tuple


# CLASSIFICATION ITERATOR
# Currently, just supports RGA like values.
# Expects files to put of the format
# r1 g1 b1 classification1
# r2 g2 b2 classification2
#         *
#         *
#         *
# rn gn bn classification


Classification = Tuple[int, int, int, int]


def __to_classification__(item: str) -> Classification:
  parts: List[int] = list(map(lambda i: int(i), item.split(" ")))
  assert(len(parts) == 4)
  return (parts[0], parts[1], parts[2], parts[3])


class Iterator(new_line.Iterator):
  identifiers = None

  def __init__(self, obj: Entry, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def from_array(cls: Any, items: List[Classification], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    content: str = cls.delimiter.item_token.join(list(map(lambda i: " ".join(list(map(lambda j: str(j), i))), items)))
    if f:
      f.write(str.encode(content))
    return (content, {})

  @classmethod
  def to_array(cls: Any, content: str) -> Iterable[Classification]:
    items = filter(lambda item: len(item.strip()) > 0, content.split(cls.delimiter.item_token))
    return map(lambda item: __to_classification__(item), items)

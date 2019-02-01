import boto3
from enum import Enum
import heapq
import util
from typing import Any, BinaryIO, ClassVar, Dict, Generic, List, Optional, Tuple, TypeVar


T = TypeVar("T")


class DelimiterPosition(Enum):
  start = 1
  inbetween = 2
  end = 3


class Delimiter:
  def __init__(self, token: str, position: DelimiterPosition):
    self.token = token
    self.position = position


class OffsetBounds:
  def __init__(self, start_index: int, end_index: int):
    assert(start_index < end_index)
    self.start_index = start_index
    self.end_index = end_index

  def __eq__(self, other):
    return self.start_index == other.start_index and self.end_index == other.end_index

  def __repr__(self):
    return "[{0:d},{1:d}]".format(self.start_index, self.end_index)


class Options:
  def __init__(self, has_header: bool):
    self.has_header = has_header


class Iterator(Generic[T]):
  adjust_chunk_size: ClassVar[int] = 300
  options: ClassVar[Options]
  read_chunk_size: ClassVar[int] = 1000*1000
  delimiter: Delimiter
  identifiers: T

  def __init__(self, cls: Any, obj: Any, offset_bounds: Optional[OffsetBounds]):
    self.cls = cls
    self.item_count = None
    self.obj = obj
    self.offset_bounds = offset_bounds
    self.offsets: List[int] = []
    self.remainder: str = ""
    self.__setup__()

  def __adjust__(self, end_index: int, delimiter: Delimiter):
    content: str = util.read(self.obj, max(end_index - self.adjust_chunk_size, 0), end_index)
    last_byte: int = len(content) - 1
    offset_index: int = last_byte - content.rindex(delimiter.token)
    assert(offset_index >= 0)
    return offset_index

  def __setup__(self):
    if self.offset_bounds:
      self.start_index = self.offset_bounds.start_index
      self.end_index = self.offset_bounds.end_index
      if self.start_index != 0:
        self.start_index -= self.__adjust__(self.start_index, self.delimiter)
        if self.delimiter.position != DelimiterPosition.start:
          # Don't include delimiter
          self.start_index += len(self.delimiter.token)
      if self.end_index != self.obj.content_length:
        self.end_index -= self.__adjust__(self.end_index, self.delimiter)
        if self.delimiter.position == DelimiterPosition.start:
          self.end_index += len(self.delimiter.identifier)
    else:
      self.start_index = 0
      self.end_index = self.obj.content_length - 1

    assert(self.start_index <= self.end_index)
    self.next_index = self.start_index
    self.content_length = self.end_index - self.start_index
    self.offsets = [self.next_index]

  @classmethod
  def combine(cls: Any, objs: List[Any], f: BinaryIO) -> Dict[str, str]:
    metadata = {}

    for i in range(len(objs)):
      obj = objs[i]
      if cls.options.has_header and i > 0:
        f.write(util.read(0, obj.content_length).split(cls.delimiter.token)[1:])
      else:
        obj.download_fileobj(f)

    return metadata

  @classmethod
  def from_array(cls: Any, items: List[str], f: BinaryIO) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    if cls.delimiter.position == DelimiterPosition.inbetween:
      content = cls.delimiter.token.join(items)
    else:
      content = "".join(items)

    f.write(content)
    return metadata

  @classmethod
  def to_array(cls: Any, content: str) -> List[str]:
    items = content.split(cls.delimiter.token)
    items = list(filter(lambda item: len(item) > 0, items))
    if cls.delimiter.position == DelimiterPosition.start:
      items = list(map(lambda item: cls.delimiter.token + item, items))
    elif cls.delimiter.position == DelimiterPosition.start:
      items = list(map(lambda item: item + cls.delimiter.token, items))
    return items

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: T) -> str:
    raise Exception("Not Implemented")

  def get(self, start_byte: int, end_byte: int) -> List[str]:
    content: str = util.read(self.obj, start_byte, end_byte)
    return self.to_array(content)

  def get_item_count(self) -> int:
    raise Exception("Not Implemented")

  def get_start_index(self) -> int:
    return self.start_index

  def get_end_index(self) -> int:
    return self.end_index

  def next(self) -> Tuple[List[str], Optional[OffsetBounds], bool]:
    next_start_index: int = self.next_index
    next_end_index: int = min(next_start_index + self.read_chunk_size, self.end_index)
    more: bool = True
    stream: str = util.read(self.obj, next_start_index, next_end_index)
    stream = self.remainder + stream

    if next_end_index == self.end_index:
      next_start_index -= len(self.remainder)
      self.remainder = ""
      more = False
    else:
      index: int = stream.rindex(self.delimiter.token) if self.delimiter.token in stream else -1
      if index != -1:
        if self.delimiter.token == DelimiterPosition.inbetween:
          index += 1
        elif self.delimiter.token == DelimiterPosition.start:
          index -= 1
        next_end_index -= (len(stream) - index)
        next_start_index -= len(self.remainder)
        self.remainder = stream[index:]
        stream = stream[:index]
      else:
        self.remainder = stream
        next_end_index -= len(self.remainder)
        stream = ""

    self.next_index = min(next_end_index + len(self.remainder) + 1, self.end_index)
    offset_bounds: Optional[OffsetBounds]
    if len(stream) == 0:
      offset_bounds = None
    else:
      offset_bounds = OffsetBounds(next_start_index, next_end_index)
    return (self.to_array(stream), offset_bounds, more)

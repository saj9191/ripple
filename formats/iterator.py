import boto3
import heapq
import os
import util
from database import Entry
from enum import Enum
from typing import Any, BinaryIO, ClassVar, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar


T = TypeVar("T")


class DelimiterPosition(Enum):
  start = 1
  inbetween = 2
  end = 3


class Delimiter:
  def __init__(self, item_token: str, offset_token: str, position: DelimiterPosition):
    self.item_token = item_token
    self.offset_token = offset_token
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
  next_index: int = -1
  options: ClassVar[Options]
  read_chunk_size: ClassVar[int] = 10*1000*1000
  delimiter: Delimiter
  identifiers: T

  def __init__(self, cls: Any, entry: Entry, offset_bounds: Optional[OffsetBounds]):
    self.cls = cls
    self.item_count = None
    self.entry = entry
    self.offset_bounds = offset_bounds
    self.offsets: List[int] = []
    self.remainder: bytes = b''
    self.__setup__()

  def __adjust__(self, end_index: int, token: str) -> int:
    content: bytes = self.entry.get_range(max(end_index - self.adjust_chunk_size, 0), end_index)
    last_byte: int = len(content) - 1
    index = content.rindex(str.encode(token))
    offset_index: int = last_byte - content.rindex(str.encode(token))
    assert(offset_index >= 0)
    return offset_index

  def __setup__(self):
    if self.offset_bounds:
      self.start_index = self.offset_bounds.start_index
      self.end_index = self.offset_bounds.end_index
      if self.start_index != 0:
        self.start_index -= self.__adjust__(self.start_index, self.delimiter.offset_token)
        if self.delimiter.position != DelimiterPosition.start:
          # Don't include delimiter
          self.start_index += len(self.delimiter.offset_token)
      if self.end_index != self.entry.content_length():
        self.end_index -= self.__adjust__(self.end_index, self.delimiter.offset_token)
        if self.delimiter.position == DelimiterPosition.start:
          self.end_index += len(self.delimiter.offset_token)
    else:
      self.start_index = 0
      self.end_index = self.entry.content_length() - 1

    assert(self.start_index <= self.end_index)
    self.content_length = self.end_index - self.start_index
    self.offsets = [self.next_index]

  @classmethod
  def combine(cls: Any, entries: List[Entry], f: BinaryIO, extra: Dict[str, Any]) -> Dict[str, str]:
    metadata: Dict[str, str] = {}

    for i in range(len(entries)):
      entry = entries[i]
      if i > 0 and cls.delimiter.position == DelimiterPosition.inbetween:
        f.seek(-1 * len(cls.delimiter.item_token), os.SEEK_END)
        end: str = f.read(len(cls.delimiter.item_token)).decode("utf-8")
        if end != cls.delimiter.item_token:
          f.write(str.encode(cls.delimiter.item_token))
      if cls.options.has_header and i > 0:
        lines = entry.get_content().split(str.encode(cls.delimiter.item_token))[1:]
        content = str.encode(cls.delimiter.item_token).join(lines)
        f.write(content)
      else:
        # TODO: There seems to be a bug where if I do entry.download(f), it's not guaranteed
        # the entire file will write at the end. I need to figure out why because downloading,
        # loading into memory and then writing to disk is slower.
        f.write(entry.get_content())

    return metadata

  @classmethod
  def from_array(cls: Any, items: List[Any], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    metadata: Dict[str, str] = {}
    if cls.delimiter.position == DelimiterPosition.inbetween:
      content = str.encode(cls.delimiter.item_token).join(items)
    else:
      content = b"".join(items)

    if f:
      f.write(content)
    return (content, metadata)

  @classmethod
  def to_array(cls: Any, content: bytes) -> Iterable[Any]:
    token = str.encode(cls.delimiter.item_token)
    items: Iterable[bytes] = filter(lambda item: len(item.strip()) > 0, content.split(token))
    if cls.delimiter.position == DelimiterPosition.start:
      items = map(lambda item: token + item, items)
    elif cls.delimiter.position == DelimiterPosition.end:
      items = map(lambda item: item + token, items)
    return items

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: T) -> float:
    raise Exception("Not Implemented")

  def get(self, start_byte: int, end_byte: int) -> Iterable[Any]:
    content: bytes = self.entry.get_range(start_byte, end_byte)
    return self.to_array(content)

  def get_extra(self) -> Dict[str, Any]:
    return {}

  def get_item_count(self) -> int:
    raise Exception("Not Implemented")

  def get_end_index(self) -> int:
    return self.end_index

  def get_start_index(self) -> int:
    return self.start_index

  def get_offset_end_index(self) -> int:
    return self.end_index

  def get_offset_start_index(self) -> int:
    return self.start_index

  def next(self) -> Tuple[Iterable[Any], Optional[OffsetBounds], bool]:
    if self.next_index == -1:
      self.next_index = self.get_offset_start_index()
    next_start_index: int = self.next_index
    next_end_index: int = min(next_start_index + self.read_chunk_size, self.get_offset_end_index())
    more: bool = True
    stream: bytes = self.entry.get_range(next_start_index, next_end_index)
    stream = self.remainder + stream
    if next_end_index == self.get_offset_end_index():
      next_start_index -= len(self.remainder)
      self.remainder = b''
      more = False
    else:
      token = str.encode(self.delimiter.offset_token)
      index: int = stream.rindex(token) if token in stream else -1
      if index != -1:
        if self.delimiter.position == DelimiterPosition.inbetween:
          index += len(self.delimiter.offset_token)
        next_end_index -= (len(stream) - index)
        next_start_index -= len(self.remainder)
        self.remainder = stream[index:]
        stream = stream[:index]
      else:
        self.remainder = stream
        next_end_index -= len(self.remainder)
        stream = b''
    self.next_index = min(next_end_index + len(self.remainder) + 1, self.get_offset_end_index())
    offset_bounds: Optional[OffsetBounds]
    if len(stream) == 0:
      offset_bounds = None
    else:
      offset_bounds = OffsetBounds(next_start_index, next_end_index)

    [stream, offset_bounds] = self.transform(stream, offset_bounds)
    return (self.to_array(stream), offset_bounds, more)

  def transform(self, stream: bytes, offset_bounds: Optional[OffsetBounds]) -> Tuple[bytes, Optional[OffsetBounds]]:
    return (stream, offset_bounds)

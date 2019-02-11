import boto3
import heapq
import iterator
import new_line
import util
from database import Entry
from iterator import OffsetBounds, Optional, Options
from typing import Any, BinaryIO, ClassVar, Dict, Iterable, List, Tuple


Neighbors = Tuple[str, List[Tuple[float, int]]]


def neighbor_from_str(item: str) -> Tuple[float, int]:
  [distance, classification] = item.split(" ")
  return (float(distance), int(classification))


def neighbor_to_str(n: Tuple[float, int]) -> str:
  return "{0:f} {1:d}".format(n[0], n[1])


def neighbors_from_str(line: str) -> Neighbors:
  items = line.split(",")
  neighbors: List[Tuple[float, int]] = list(map(lambda item: neighbor_from_str(item), items[1:]))
  return (items[0], neighbors)


def neighbors_to_str(n: Neighbors) -> str:
  s: str = n[0]
  neighbors = list(map(lambda i: neighbor_to_str(i), n[1]))
  return s + "," + ",".join(neighbors)


class Iterator(new_line.Iterator):
  identifiers = None
  options: ClassVar[Options] = Options(has_header=False)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def from_array(cls: Any, items: List[Neighbors], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    content = cls.delimiter.item_token.join(list(map(lambda n: neighbors_to_str(n), items)))

    if f:
      f.write(str.encode(content))
    return (content, {})

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: None) -> float:
    # I don't think an identifier makes sense for knn. However, this may change
    # if we ever come up with a reason to sort the values.
    raise Exception("Not implemented")

  @classmethod
  def to_array(cls: Any, content: str) -> Iterable[Neighbors]:
    return map(lambda n: neighbors_from_str(n), content.split(cls.delimiter.item_token))

  @classmethod
  def combine(cls: Any, entries: List[Entry], f: BinaryIO, extra: Dict[str, Any]) -> Dict[str, str]:
    top_scores = {}
    for entry in entries:
      items = cls.to_array(entry.get_content())
      for item in items:
        [s, neighbors] = item
        if s not in top_scores:
          top_scores[s] = list(map(lambda n: (-1 * n[0], n[1]), neighbors))
          heapq.heapify(top_scores[s])
        else:
          for neighbor in neighbors:
            [score, classification] = neighbor
            if len(top_scores[s]) < extra["n"] or -1*score > top_scores[s][0][0]:
              heapq.heappush(top_scores[s], (-1*score, classification))
            if len(top_scores[s]) > extra["n"]:
              heapq.heappop(top_scores[s])

    for s in top_scores.keys():
      line = s
      for [score, c] in top_scores[s]:
        line += ",{0:f} {1:d}".format(-1 * score, c)
      f.write(str.encode("{0:s}\n".format(line)))

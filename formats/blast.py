import iterator
import re
import util
from enum import Enum
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Optional
from typing import Any, ClassVar, Generic


class Identifiers(Enum):
  score = 0


class Iterator(iterator.Iterator[Identifiers]):
  delimiter: Delimiter = Delimiter(item_token="\n\n", offset_token="\n\n", position=DelimiterPosition.end)
  identifiers: Identifiers
  optimal_score_regex = re.compile("[\S\s]*^optimal_alignment_score:\s(\d+)[\S\s]*", re.MULTILINE)
  suboptimal_score_regex = re.compile("[\S\s]*suboptimal_alignment_score:\s(\d+)[\S\s]*", re.MULTILINE)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: Identifiers) -> float:
    m = cls.optimal_score_regex.match(item)
    score = float(m.group(1)) * 1000
    m = Iterator.suboptimal_score_regex.match(item)
    if m is not None:
      score += float(m.group(1))
    return score

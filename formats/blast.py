import iterator
import re


class Iterator(iterator.Iterator):
  IDENTIFIER = "\n\n"
  OPTIMAL_SCORE_REGEX = re.compile("[\S\s]*^optimal_alignment_score:\s(\d+)[\S\s]*", re.MULTILINE)
  SUBOPTIMAL_SCORE_REGEX = re.compile("[\S\s]*suboptimal_alignment_score:\s(\d+)[\S\s]*", re.MULTILINE)

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, Iterator, obj, batch_size, chunk_size)
    self.identifier = Iterator.IDENTIFIER
    self.offsets = [0]

  def fromArray(items, includeHeader=False):
    return Iterator.IDENTIFIER.join(items)

  def getScore(b):
    m = Iterator.OPTIMAL_SCORE_REGEX.match(b)
    score = int(m.group(1)) * 1000
    m = Iterator.SUBOPTIMAL_SCORE_REGEX.match(b)
    if m is not None:
      score += int(m.group(1))
    return score

  def get(obj, start_byte, end_byte, identifier=False):
    content = Iterator.getBytes(obj, start_byte, end_byte)
    blast = content.split(Iterator.IDENTIFIER)
    blast = list(filter(lambda b: len(b.strip()) > 0, blast))
    if identifier:
      try:
        blast = list(map(lambda b: (Iterator.getScore(b), b), blast))
      except Exception as e:
        print(obj)
        raise e
    return blast

  def more(self):
    return self.current_offset < self.content_length

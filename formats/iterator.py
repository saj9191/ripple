import boto3
import heapq
import util


class Element():
  def __init__(self, values, more, iterator):
    self.values = values
    self.more = more
    self.iterator = iterator

  def __lt__(self, other):
    return self.values[0][0] < other.values[0][0]


class Iterator:
  def __init__(self, cls, obj, batch_size, chunk_size):
    self.batch_size = batch_size
    self.chunk_size = chunk_size
    self.content_length = obj.content_length
    self.current_offset = 0
    self.seen_count = 0
    self.total_count = None
    self.remainder = ""
    self.offsets = []
    self.obj = obj
    self.cls = cls

  # This is for the sort function
  def __lt__(self, other):
    return True

  def createContent(self, content):
    raise Exception("Not Implemented")

  def fromArray(obj, items, offsets):
    raise Exception("Not Implemented")

  def getCount(self):
    raise Exception("Not Implemented")

  @classmethod
  def write(cls, f, values, first):
    content = cls.fromArray(None, values, None)
    if first:
      f.write(content.strip())
    else:
      f.write(cls.IDENTIFIER)
      f.write(content.strip())

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    s3 = boto3.resource("s3")
    iterators = []
    values = []
    if util.is_set(params, "sort"):
      for key in keys:
        obj = s3.Object(bucket_name, key)
        iterator = cls(obj, {}, params["batch_size"], params["chunk_size"])
        [s, more] = iterator.next(params["identifier"])
        if len(s) > 0:
          heapq.heappush(iterators, Element(s, more, iterator))
    else:
      raise Exception("Not implementeD")

    last = None
    with open(temp_name, "w+") as f:
      first = True
      while util.is_set(params, "sort") and len(iterators) > 0:
        element = heapq.heappop(iterators)
        next_value = element.values.pop(0)
        assert(last is None or last <= next_value[0])
        last = next_value[0]
        values.append(next_value[1])
        if len(element.values) > 0:
          heapq.heappush(iterators, element)
        elif element.more:
          [s, more] = element.iterator.next(params["identifier"])
          element.values = s
          element.more = more
          heapq.heappush(iterators, element)

        if len(values) == params["batch_size"]:
          cls.write(f, values, first)
          first = False
          values = []

      if len(values) > 0:
        cls.write(f, values, first)

  def next(self, identifier=""):
    [o, more] = self.nextOffsets()
    if len(o["offsets"]) == 0:
      return [[], False]
    return [self.cls.get(self.obj, o["offsets"][0], o["offsets"][-1], identifier), more]

  def nextOffsets(self):
    if self.content_length == 0:
      return ({"offsets": []}, False)
    # Plus one is so we get end byte of value
    while len(self.offsets) <= 1 and self.current_offset < self.content_length:
      self.updateOffsets()

    if len(self.offsets) == 0 and self.current_offset >= self.content_length:
      return ({"offsets": []}, False)

    start = self.offsets[0]
    if len(self.offsets) == 1:
      end = self.content_length
      self.offsets = []
    else:
      end = self.offsets[1]
      if self.offsets[1] + 1 >= self.content_length:
        self.offsets = []
      else:
        self.offsets = self.offsets[1:]
        self.offsets[0] = end + 1

    o = {"offsets": [start, end]}
    return (o, self.more())

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(start_byte + self.chunk_size, self.content_length)
    stream = util.read(self.obj, start_byte, end_byte)
    stream = self.remainder + stream
    start_byte -= len(self.remainder)
    index = stream.rindex(self.identifier) if self.identifier in stream else -1
    if index != -1:
      #self.offsets.append(start_byte)
      self.offsets.append(start_byte + index)
      start_byte += index + 1
      self.current_offset = end_byte + 1
      stream = stream[index + 1:]
    else:
      if end_byte == self.content_length:
        self.offsets.append(self.content_length)
        self.current_offset = self.content_length
      else:
        self.current_offset = start_byte
    self.remainder = stream

  def more(self):
    return len(self.offsets) > 0 or self.current_offset < self.content_length

  def endByte(self):
    return self.current_offset

import boto3
import heapq


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

  def fromArray(items, includeHeader=False):
    raise Exception("Not Implemented")

  def getCount(self):
    raise Exception("Not Implemented")

  @classmethod
  def write(cls, f, values, first):
    content = cls.fromArray(values, includeHeader=first)
    if first:
      f.write(content.strip())
    else:
      f.write(cls.IDENTIFIER)
      f.write(content.strip())

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    print("Combining", len(keys))
    s3 = boto3.resource("s3")
    iterators = []
    values = []
    if params["sort"]:
      for key in keys:
        obj = s3.Object(bucket_name, key)
        iterator = cls(obj, params["batch_size"], params["chunk_size"])
        [s, more] = iterator.next(identifier=True)
        if len(s) > 0:
          heapq.heappush(iterators, Element(s, more, iterator))
    else:
      raise Exception("Not implementeD")

    last = None
    with open(temp_name, "w+") as f:
      first = True
      while params["sort"] and len(iterators) > 0:
        element = heapq.heappop(iterators)
        next_value = element.values.pop(0)
        assert(last is None or last <= next_value[0])
        last = next_value[0]
        values.append(next_value[1])
        if len(element.values) > 0:
          heapq.heappush(iterators, element)
        elif more:
          [s, more] = element.iterator.next(identifier=True)
          element.values = s
          element.more = more
          heapq.heappush(iterators, element)

        if len(values) == params["batch_size"]:
          cls.write(f, values, first)
          first = False
          values = []

      if len(values) > 0:
        cls.write(f, values, first)

  def getBytes(obj, start_byte, end_byte):
    return obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")

  def next(self, identifier=""):
    [start_byte, end_byte, more] = self.nextOffsets()
    if start_byte == -1:
      return [[], more]
    return [self.cls.get(self.obj, start_byte, end_byte, identifier), more]

  def nextOffsets(self):
    if self.content_length == 0:
      return (-1, -1, False)
    # Plus one is so we get end byte of value
    while len(self.offsets) < (self.batch_size + 1) and self.current_offset < self.content_length:
      self.updateOffsets()

    if len(self.offsets) == 0 and self.current_offset >= self.content_length:
      return (-1, -1, False)

    start_offset = self.offsets[0]
    if len(self.offsets) > self.batch_size:
      end_offset = self.offsets[self.batch_size] - 1
    else:
      end_offset = self.endByte()
    self.seen_count += min(len(self.offsets), self.batch_size)
    self.offsets = self.offsets[self.batch_size:]
    return (start_offset, end_offset, self.more())

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(start_byte + self.chunk_size, self.content_length)
    stream = Iterator.getBytes(self.obj, start_byte, end_byte)
    stream = self.remainder + stream
    start_byte -= len(self.remainder)
    done = False
    while not done:
      index = stream.index(self.identifier) if self.identifier in stream else -1
      done = (index == -1)
      if index != -1:
        offset = index + len(self.identifier)
        self.offsets.append(start_byte + offset)
        start_byte += offset
        stream = stream[offset:]
        self.current_offset = start_byte
      else:
        if end_byte == self.content_length:
          self.offsets.append(self.content_length)
          self.current_offset = self.content_length
        else:
          self.current_offset = start_byte
      self.remainder = stream

  def more(self):
    return self.seen_count < self.total_count

  def endByte(self):
    return self.current_offset

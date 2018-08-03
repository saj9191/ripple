import iterator


class Iterator(iterator.Iterator):
  IDENTIFIER = "\n"

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, Iterator, obj, batch_size, chunk_size)
    self.identifier = Iterator.IDENTIFIER
    start_byte = 0
    end_byte = start_byte + self.chunk_size
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
    self.current_offset = stream.indexOf(Iterator.IDENTIFIER) + 1
    # First element
    self.offsets.append(self.current_offset)
    self.getCount()

  def getCount(self):
    if self.total_count is not None:
      return self.total_count

    start_byte = self.current_offset
    count = 0
    while start_byte < self.content_length:
      end_byte = start_byte + self.chunk_size
      stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
      count += stream.count(Iterator.IDENTIFIER)
      start_byte = end_byte + 1

    # Need to account for header line
    if stream.endswith(Iterator.IDENTIFIER):
      self.total_count = count
    else:
      self.total_count = count - 1

  def fromArray(items):
    return Iterator.IDENTIFIER.join(items)

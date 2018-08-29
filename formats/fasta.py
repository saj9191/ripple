import iterator
import util


class Iterator(iterator.Iterator):
  ENTRY_IDENTIFIER = ">"

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, obj, batch_size, chunk_size)
    self.total_count = None
    self.offsets = []
    self.current_offset = 0

  def getCount(self):
    if self.total_count is not None:
      return self.total_count

    start_byte = self.current_offset
    count = 0
    while start_byte < self.content_length:
      end_byte = start_byte + self.chunk_size
      stream = util.read(self.obj, start_byte, end_byte)
      count += stream.count(self.ENTRY_IDENTIFIER)
      start_byte = end_byte + 1

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(start_byte + self.chunk_size, self.content_length)
    stream = util.read(self.obj, start_byte, end_byte)
    done = False
    while not done:
      index = stream.index(self.ENTRY_IDENTIFIER)
      done = (index == -1)
      if index != -1:
        self.offsets.append(start_byte + index)
        stream = stream[index + 1:]
    self.current_offset = end_byte + 1

  def createContent(self, content):
    raise Exception("Not Implemented")

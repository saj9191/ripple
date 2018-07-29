class Iterator:
  def __init__(self, obj, batch_size, chunk_size):
    self.batch_size = batch_size
    self.chunk_size = chunk_size
    self.content_length = obj.content_length
    self.obj = obj

  def getBytes(obj, start_byte, end_byte):
    return obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")

  # This is for the sort function
  def __lt__(self, other):
    return True

  def getCount(self):
    raise Exception("Not Implemented")

  def nextOffsets(self):
    # Plus one is so we get end byte of spectra
    while len(self.offsets) < (self.batch_size + 1) and self.current_spectra_offset < self.content_length:
      self.updateOffsets()

    start_offset = self.offsets[0]
    if len(self.offsets) > self.batch_size:
      end_offset = self.offsets[self.batch_size] - 1
    else:
      end_offset = self.spectra_list_offset

    self.seen_count += min(len(self.offsets), self.batch_size)
    self.offsets = self.offsets[self.batch_size:]
    return (start_offset, end_offset, self.seen_count < self.total_count)

  def createContent(self, content):
    raise Exception("Not Implemented")

  def from_array(items):
    raise Exception("Not Implemented")

class Iterator:
  def __init__(self, obj, batch_size, chunk_size):
    self.batch_size = batch_size
    self.chunk_size = chunk_size
    self.obj = obj

  def getBytes(self, start_byte, end_byte):
    return self.obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")

  def getCount(self):
    raise Exception("Not Implemented")

  def nextFile(self):
    raise Exception("Not Implemented")

  def findOffsets(self):
    raise Exception("Not Implemented")

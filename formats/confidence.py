import percolator


class Iterator(percolator.Iterator):
  QVALUE_INDEX = 9

  def __init__(self, obj, batch_size, chunk_size):
    percolator.Iterator.__init__(self, obj, batch_size, chunk_size)
    self.cls = Iterator

import iterator


class Iterator(iterator.Iterator):
  IDENTIFIER = ">"

  def __init__(self, obj, offsets, batch_size, chunk_size):
    self.identifier = Iterator.IDENTIFIER
    iterator.Iterator.__init__(self, obj, batch_size, chunk_size)
    self.indicator_at_beginning = True
    iterator.Iterator.__setup__(self, offsets)

  def fromArray(items, includeHeader=False):
    assert(not includeHeader)
    items = list(map(lambda item: item.strip(), items))
    content = Iterator.IDENTIFIER.join(items)
    return Iterator.IDENTIFIER + content

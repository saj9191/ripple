import boto3
import iterator


def combine(bucket_name, keys, temp_name, params):
  if params["sort"]:
    raise "Not Implemented"

  s3 = boto3.resource("s3")
  with open(temp_name, "w+") as f:
    for i in range(len(keys)):
      key = keys[i]
      content = s3.Object(bucket_name, key).get()["Body"].read().decode("utf-8")
      if i == 0:
        f.write(content)
      else:
        results = content.split("\n")[1:]
        f.write("\n".join(results))


class Iterator(iterator.Iterator):
  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, obj, batch_size, chunk_size)
    self.total_count = None
    self.offsets = []

    start_byte = 0
    end_byte = start_byte + self.chunk_size
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
    self.current_offset = stream.indexOf("\n") + 1
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
      count += stream.count("\n")
      start_byte = end_byte + 1

    # Need to account for header line
    if stream.endswith("\n"):
      self.total_count = count
    else:
      self.total_count = count - 1

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(start_byte + self.chunk_size, self.content_length)
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
    done = False
    while not done:
      index = stream.index("\n")
      done = (index == -1)
      if index != -1:
        self.offsets.append(start_byte + index + 1)
        stream = stream[index + 1:]
    self.current_offset = end_byte + 1

  def createContent(self, content):
    raise Exception("Not Implemented")

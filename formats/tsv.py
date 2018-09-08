import boto3
import iterator
import util


class Iterator(iterator.Iterator):
  IDENTIFIER = "\n"
  COLUMN_SEPARATOR = "\t"
  HEADER_ITEMS = [
    "file",
    "scan",
    "charge",
    "spectrum precursor m/z",
    "spectrum neutral mass",
    "peptide mass",
    "delta_cn",
    "delta_lcn",
    "xcorr score",
    "xcorr rank",
    "distinct matches/spectrum",
    "sequence",
    "modifications",
    "cleavage type",
    "protein id",
    "flanking aa",
    "target/decoy",
    "original target sequence"
  ]

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, Iterator, obj, batch_size, chunk_size)
    self.identifier = Iterator.IDENTIFIER
    start_byte = 0
    end_byte = start_byte + self.chunk_size
    stream = util.read(self.obj, start_byte, end_byte)
    self.current_offset = stream.index(Iterator.IDENTIFIER) + 1
    self.offsets = [self.current_offset]

  def fromArray(items, includeHeader=False):
    items = list(map(lambda item: item.strip(), items))
    content = Iterator.IDENTIFIER.join(items)
    if includeHeader:
      content = "\t".join(Iterator.HEADER_ITEMS) + "\n" + content
    return content

  def get(obj, start_byte, end_byte, identifier=""):
    content = util.read(obj, start_byte, end_byte)
    items = list(content.split(Iterator.IDENTIFIER))
    if identifier:
      raise Exception("TSV score identifier not implemented")
    return items

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    s3 = boto3.resource("s3")
    if params["sort"]:
      raise Exception("Not implement")

    with open(temp_name, "w+") as f:
      f.write("\t".join(Iterator.HEADER_ITEMS))
      for i in range(len(keys)):
        key = keys[i]
        obj = s3.Object(bucket_name, key)
        content = util.read(obj, 0, obj.content_length)
        if i == 0:
          f.write(content)
        else:
          f.write(content[content.find("\n") + 1:])

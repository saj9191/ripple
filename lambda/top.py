import heapq
import importlib
import util


class Element:
  def __init__(self, identifier, value):
    self.value = value
    self.identifier = identifier

  def __lt__(self, other):
    return self.identifier < other.identifier


def find_top(bucket_name, key, input_format, output_format, offsets, params):
  s3 = params["s3"]
  obj = s3.Object(bucket_name, key)
  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  it = iterator(obj, params["chunk_size"], offsets)
  offsets = it.format_offsets(offsets)

  top = []
  more = True
  identifier = params["identifier"] if "identifier" in params else None
  while more:
    [values, more] = it.next(identifier=identifier)

    for value in values:
      heapq.heappush(top, Element(value[0], value[1]))
      if len(top) > params["number"]:
        heapq.heappop(top)

  values = list(map(lambda t: t.value, top))
  content = iterator.fromArray(obj, values, offsets)

  file_name = util.file_name(output_format)
  util.write(output_format, bucket_name, file_name, str.encode(content), params)


def handler(event, context):
  util.handle(event, context, find_top)

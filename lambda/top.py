import boto3
import heapq
import importlib
import util


class Element:
  def __init__(self, identifier, value):
    self.value = value
    self.identifier = identifier

  def __lt__(self, other):
    return self.identifier < other.identifier


def find_top(bucket_name, key, input_format, output_format, start_byte, end_byte, params):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  it = iterator(obj, params["batch_size"], params["chunk_size"])

  top = []
  more = True
  identifier = params["identifier"] if "identifier" in params else None
  while more:
    if start_byte == 0 and end_byte == obj.content_length:
      [values, more] = it.next(identifier=identifier)
    else:
      values = iterator.get(obj, start_byte, end_byte, identifier)
      more = False

    for value in values:
      heapq.heappush(top, Element(value[0], value[1]))
      if len(top) > params["number"]:
        heapq.heappop(top)

  content = iterator.fromArray(list(map(lambda t: t.value, top)))

  file_name = util.file_name(output_format)
  util.print_write(output_format, file_name, params)
  s3.Object(bucket_name, file_name).put(Body=str.encode(content))


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, find_top)
  util.show_duration(context, m, params)

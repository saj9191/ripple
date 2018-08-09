import boto3
import heapq
import importlib
import util


def find_top(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  it = iterator(obj, params["batch_size"], params["chunk_size"])

  top = []
  more = True
  identifier = params["identifier"] if "identifier" in params else None
  while more:
    [spectra, more] = it.next(identifier=identifier)
    for spectrum in spectra:
      heapq.heappush(top, spectrum)
      if len(top) > params["number"]:
        heapq.heappop(top)

  content = iterator.fromArray(list(map(lambda t: t[1], top)))

  p = dict(m)
  p["prefix"] = m["prefix"] + 1
  file_name = util.file_name(p)
  util.print_write(m, file_name, params)
  s3.Object(bucket_name, file_name).put(Body=str.encode(content))

  return m


def run(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  if "range" in params["extra_params"]:
    rparams = params["extra_params"]["range"]
    start_byte = rparams["start_byte"]
    end_byte = rparams["end_byte"]
    file_id = rparams["file_id"]
    more = rparams["more"]

    m["last"] = not more
    m["file_id"] = file_id
    sort(bucket_name, key, m, start_byte, end_byte, pivots, params)
  else:
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    sort(bucket_name, key, m, 0, obj.content_length, pivots, params)
  return m


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = find_top(bucket_name, key, params)
  util.show_duration(context, m, params)

import boto3
import heapq
import importlib
import util


def find_top(bucket_name, key, m, start_byte, end_byte, params):
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
      [spectra, more] = it.next(identifier=identifier)
    else:
      it.get(obj, start_byte, end_byte, identifier=identifier)
      more = False

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


def run(bucket_name, key, params, func):
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
    func(bucket_name, key, m, start_byte, end_byte, params)
  else:
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    func(bucket_name, key, m, 0, obj.content_length, params)
  return m


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = run(bucket_name, key, params, find_top)
  util.show_duration(context, m, params)

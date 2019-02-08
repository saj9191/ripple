import importlib
import util
from database import Database
from iterator import OffsetBounds
from typing import Any, Dict, List


def run_application(d: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  temp_file = "/tmp/{0:s}".format(key)
  util.make_folder(util.parse_file_name(key))

  if len(offsets) == 0:
    with open(temp_file, "wb+") as fb:
      d.download(bucket_name, key, fb)
  else:
    obj = d.Key(bucket_name, key)
    format_lib = importlib.import_module(params["format"])
    iterator_class = getattr(format_lib, "Iterator")
    iterator = iterator_class(obj, OffsetBounds(offsets[0], offsets[1]))
    items = iterator.get(iterator.get_start_index(), iterator.get_end_index())
    with open(temp_file, "wb+") as f:
      items = list(items)
      iterator_class.from_array(list(items), f, iterator.get_extra())

  application_lib = importlib.import_module(params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(temp_file, params, input_format, output_format, offsets)

  for output_file in output_files:
    p = util.parse_file_name(output_file.replace("/tmp/", ""))
    if p is None:
      index = output_file.rfind(".")
      ext = output_file[index+1:]
      output_format["ext"] = ext
      new_key = util.file_name(output_format)
    else:
      new_key = util.file_name(p)

    util.write(params["bucket"], new_key, open(output_file, "rb"), {}, params)


def handler(event, context):
  util.handle(event, context, run_application)

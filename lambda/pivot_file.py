import boto3
import importlib
import util
from iterator import OffsetBounds
from typing import List


def create_pivots(s3, format_lib, iterator_class, items, params) -> List[float]:
  if len(items) == 0:
    return []

  pivots: List[float] = list(map(lambda item: iterator_class.get_identifier_value(item, format_lib.Identifiers[params["identifier"]]), items))
  pivots.sort()

  max_identifier: float = float(pivots[-1] + 1)
  num_bins = 2 * params["num_bins"]
  increment = int((len(items) + num_bins - 1) / num_bins)
  pivots = pivots[0::increment]
  if pivots[-1] == max_identifier - 1:
    pivots[-1] = max_identifier
  else:
    pivots.append(max_identifier)
  return pivots


def handle_pivots(bucket_name, key, input_format, output_format, offsets, params):
  s3 = params["s3"]
  obj = s3.Object(bucket_name, key)

  format_lib = importlib.import_module(params["format"])
  iterator_class = getattr(format_lib, "Iterator")
  if len(offsets) > 0:
    it = iterator_class(obj, OffsetBounds(offsets[0], offsets[1]))
  else:
    it = iterator_class(obj, None)

  items = it.get(it.get_start_index(), it.get_end_index())
  pivots: List[float] = create_pivots(s3, format_lib, iterator_class, list(items), params)

  output_format["ext"] = "pivot"
  pivot_key = util.file_name(output_format)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  util.write(params["bucket"], pivot_key, content, {}, params)


def handler(event, context):
  util.handle(event, context, handle_pivots)

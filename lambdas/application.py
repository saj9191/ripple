# This file is part of Ripple.

# Ripple is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Ripple is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Ripple.  If not, see <https://www.gnu.org/licenses/>.

import importlib
import util
from formats.iterator import OffsetBounds
from typing import Any, Dict, List


def run_application(database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  temp_file = "/tmp/{0:s}".format(key)
  util.make_folder(util.parse_file_name(key))

  if len(offsets) == 0:
    database.download(bucket_name, key, temp_file)
  else:
    obj = database.get_entry(bucket_name, key)
    format_lib = importlib.import_module("formats." + params["input_format"])
    iterator_class = getattr(format_lib, "Iterator")
    iterator = iterator_class(obj, OffsetBounds(offsets[0], offsets[1]))
    items = iterator.get(iterator.get_start_index(), iterator.get_end_index())
    with open(temp_file, "wb+") as f:
      items = list(items)
      iterator_class.from_array(list(items), f, iterator.get_extra())

  application_lib = importlib.import_module("applications." + params["application"])
  application_method = getattr(application_lib, "run")
  output_files = application_method(database, temp_file, params, input_format, output_format)

  found = False
  for output_file in output_files:
    p = util.parse_file_name(output_file.replace("/tmp/", ""))
    if p is None:
      index = output_file.rfind(".")
      ext = output_file[index+1:]
      output_format["ext"] = ext
      new_key = util.file_name(output_format)
    else:
      new_key = util.file_name(p)

    with open(output_file, "rb") as f:
      database.put(params["bucket"], new_key, f, {})
  return True


def main(*argv):
  util.handle(argv, run_application)

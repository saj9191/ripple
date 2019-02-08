import importlib
import inspect
import os
import sys
import tutils
from typing import Any, Dict, List, Set
from tutils import Bucket, Object


class Pipeline:
  def __init__(self, params: Dict[str, Any]):
    self.setup = self.__import_base__("setup")
    self.util = self.__import_base__("util")
    self.setup.process_functions(params)
    self.params = params
    self.bucket_name = params["bucket"]
    self.log_name = params["log"]
    self.functions = params["functions"]
    self.pipeline = params["pipeline"]
    self.log = Bucket(self.log_name, [])

  def __import_format__(self, module) -> Any:
    return self.__import_module__("formats", module)

  def __import_function__(self, module) -> Any:
    return self.__import_module__("lambda", module)

  def __import_module__(self, folder, module) -> Any:
    current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir + "/" + folder)
    lib = importlib.import_module(module)
    sys.path.insert(0, current_dir)
    return lib 

  def __import_base__(self, module) -> Any:
    current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir)
    lib = importlib.import_module(module)
    sys.path.insert(0, current_dir)
    return lib

  def __trigger__(self, key: str):
    stage: int = self.util.parse_file_name(key)["prefix"]
    function_name: str = self.pipeline[stage]["name"]
    print("function_name", function_name)
    print(self.functions[function_name])
    function_params: Dict[str, Any] = { **self.params, **self.pipeline[stage], **self.functions[function_name] }

    if "format" in function_params:
      self.__import_format__(function_params["format"])

    function_module = self.__import_function__(function_params["file"])
    obj = Object(key, open("/tmp/{0:s}".format(key)).read())
    self.bucket.objects.objects.append(obj)    

    event = tutils.create_event(self.bucket_name, obj.key, [self.bucket, self.log], function_params)
    context = tutils.create_context({ "timeout": 60 })
    function_module.handler(event, context)

  # Okay so we need to look at payloads instead.
  # So it may be better to try to create the S3 wrapper first.
  def run(self, key: str, content: str):
    complete_keys = set()
    self.util.make_folder(self.util.parse_file_name(key))
    with open("/tmp/{0:s}".format(key), "w+") as f:
      f.write(content)
    obj = Object(key, content) 
    self.bucket = Bucket(self.bucket_name, [obj])
    pending_keys: List[str] = [obj.key]

    while len(pending_keys) > 0:
      self.__trigger__(pending_keys[0])
      all_keys: Set[str] = set(list(map(lambda obj: obj.key, self.bucket.objects.objects)))
      pending_keys = sorted(list(all_keys.difference(complete_keys)))

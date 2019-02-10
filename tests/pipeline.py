import importlib
import inspect
import json
import os
import shutil
import sys
import time
import tutils
import util
from typing import Any, Dict, List, Set
from tutils import TestDatabase, TestEntry, TestTable


class Pipeline:
  database: TestDatabase
  dir_name: str
  log: TestTable
  log_name: str
  params: Dict[str, Any]
  stage: int
  table: TestTable
  table_name: str

  def __init__(self, json_path: str):
    with open(json_path) as f:
      self.params = json.loads(f.read())

    self.stage = -1
    self.dir_name = "test-{0:f}".format(time.time())
    self.setup = self.__import_base__("setup")
    self.util = self.__import_base__("util")
    self.setup.process_functions(self.params)
    self.table_name = self.params["bucket"]
    self.log_name = self.params["log"]
    self.functions = self.params["functions"]
    self.pipeline = self.params["pipeline"]
    self.database = TestDatabase()
    self.table = self.database.add_table(self.table_name)
    self.log = self.database.add_table(self.log_name)
    os.mkdir(self.dir_name)

  def __del__(self):
    shutil.rmtree(self.dir_name)

  def __get_stage__(self, payload: Dict[str, Any]) -> int:
    s3 = payload["Records"][0]["s3"]
    if "extra_params" in s3:
      extra_params = s3["extra_params"]
      if "prefix" in extra_params:
        return extra_params["prefix"]
    return util.parse_file_name(s3["object"]["key"])["prefix"]

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

  def __import_application__(self, module) -> Any:
    return self.__import_module__("applications", module)

  def __trigger__(self, payload: Dict[str, Any]):
    stage: int = self.__get_stage__(payload)
    if stage == len(self.pipeline):
      print("Pipeline Finished")
      return

    function_name: str = self.pipeline[stage]["name"]
    if stage > self.stage:
      print("Starting stage {0:d}: {1:s}".format(stage + 1, function_name))
      self.stage = stage
    function_params: Dict[str, Any] = {**self.params, **self.pipeline[stage], **self.functions[function_name]}

    if "format" in function_params:
      self.__import_format__(function_params["format"])
    if "application" in function_params:
      self.__import_application__(function_params["application"])
    function_module = self.__import_function__(function_params["file"])
    event = tutils.create_event_from_payload(self.database, payload, function_params)
    context = tutils.create_context({"timeout": 60})
    function_module.handler(event, context)

  def __clean_up__(self, token):
    for i in range(len(self.params["pipeline"]) + 1):
      directory = "{0:d}/{1:s}".format(i, token)
      if os.path.isdir(directory):
        shutil.rmtree(directory)

  def populate_table(self, table_name: str, prefix: str, files: List[str]):
    table: TestTable = self.database.add_table(table_name)
    for file in files:
      with open(file, "rb") as f:
        table.add_entry(file.replace(prefix, ""), f.read())

  # Okay so we need to look at payloads instead
  # So it may be better to try to create the S3 wrapper first.
  def run(self, key: str, content: str):
    token: str = key.split("/")[1]
    entry: TestEntry = self.table.add_entry(key, content)
    self.database.payloads.append(tutils.create_payload(self.table_name, key, 0))

    while len(self.database.payloads) > 0:
      self.database.payloads = sorted(self.database.payloads, key=lambda payload: self.__get_stage__(payload))
      payload = self.database.payloads.pop(0)
      self.__trigger__(payload)

    self.__clean_up__(token)

import importlib
import inspect
import json
import os
import queue
import shutil
import sys
import time
import tutils
import util
from threading import Thread
from typing import Any, Dict, List, Set
from tutils import TestDatabase, TestEntry, TestTable


def get_stage(payload: Dict[str, Any]) -> int:
  s3 = payload["Records"][0]["s3"]
  if "extra_params" in s3:
    extra_params = s3["extra_params"]
    if "prefix" in extra_params:
      return extra_params["prefix"]
  return util.parse_file_name(s3["object"]["key"])["prefix"]


class Worker(Thread):
  def __init__(self, database, functions, params, pipeline, task_queue):
    Thread.__init__(self)
    self.database = database
    self.functions = functions
    self.params = params
    self.pipeline = pipeline
    self.running = True
    self.stage = -1
    self.task_queue = task_queue

  def __import_function__(self, module) -> Any:
    return self.__import_module__("lambda", module)

  def __import_module__(self, folder, module) -> Any:
    current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir + "/" + folder)
    lib = importlib.import_module(module)
    sys.path.insert(0, current_dir)
    return lib 

  def __trigger__(self, payload: Dict[str, Any]):
    stage: int = get_stage(payload)
    if stage > self.stage:
      self.stage = stage
    if self.stage == len(self.pipeline):
      self.task_queue.put(payload)
      return
    function_name: str = self.pipeline[stage]["name"]
    function_params: Dict[str, Any] = {**self.params, **self.pipeline[stage], **self.functions[function_name]}

    function_module = self.__import_function__("lambdas." + function_params["file"])
    self.database.params = function_params
    event = tutils.create_event_from_payload(self.database, payload)
    context = tutils.create_context({"timeout": 60})
    function_module.main(event, context)

  def run(self):
    try:
      while self.stage < len(self.pipeline):
        payload = self.task_queue.get()
        self.__trigger__(payload)
    except:
      self.running = False
      os.exit(1)
    self.running = False


class Pipeline:
  database: TestDatabase
  log: TestTable
  log_name: str
  num_threads: int
  params: Dict[str, Any]
  stage: int
  table: TestTable
  table_name: str

  def __init__(self, json_path: str, num_threads: int = 1):
    self.dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(self.dir_path + "/" + json_path) as f:
      self.params = json.loads(f.read())

    self.database = TestDatabase()
    self.functions = self.params["functions"]
    self.log_name = self.params["log"]
    self.log = self.database.add_table(self.log_name)
    self.num_threads = num_threads
    self.pipeline = self.params["pipeline"]
    self.table_name = self.params["bucket"]
    self.table = self.database.add_table(self.table_name)
    self.task_queue = queue.Queue()
    self.__setup_threads__()

  def __clean_up__(self, token):
    for i in range(len(self.params["pipeline"]) + 1):
      directory = "{0:d}/{1:s}".format(i, token)
      if os.path.isdir(directory):
        shutil.rmtree(directory)

  def __setup_threads__(self):
    self.threads = []
    for i in range(self.num_threads):
      self.threads.append(Worker(self.database, self.functions, self.params, self.pipeline, self.task_queue))
      self.threads[-1].start()

  def populate_table(self, table_name: str, prefix: str, files: List[str]):
    table: TestTable = self.database.add_table(table_name)
    for file in files:
      with open(self.dir_path + "/" + prefix + file, "rb") as f:
        table.add_entry(file, f.read())

  # Okay so we need to look at payloads instead
  # So it may be better to try to create the S3 wrapper first.
  def run(self, key: str, file: str):
    with open(self.dir_path + "/" + file, "rb") as f:
      content: bytes = f.read()
    token: str = key.split("/")[1]
    entry: TestEntry = self.table.add_entry(key, content)
    self.database.payloads.append(tutils.create_payload(self.table_name, key, 0))
    self.database.payloads[-1]["execute"] = 0

    while len(self.threads) > 0:
      self.database.payloads = sorted(self.database.payloads, key=lambda payload: get_stage(payload))
      while len(self.database.payloads) > 0:
        self.task_queue.put(self.database.payloads.pop(0))

      i = 0
      while i < len(self.threads):
        if self.threads[i].running:
          i += 1
        else:
          self.threads[i].join()
          self.threads.pop(i)
      time.sleep(1)

    print("Pipeline Finished")
    self.database.statistics.calculate_total_cost()
    self.__clean_up__(token)

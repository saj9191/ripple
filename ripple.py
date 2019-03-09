import importlib
import inspect
import json
import os
import re
import setup
import sys
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
from typing import Any, Dict, Optional


IMPORT_REGEXES = [re.compile(r"import ([a-zA-Z\_\.]+)"), re.compile("from ([a-zA-Z\_\.]+) import .*")]
SUPPORTED_LIBRARIES = set(["PIL", "numpy", "sklearn", "thundra"])


def new(name: str, table: str, log: str, timeout: int, config: Dict[str, Any]):
  pipeline = Pipeline(name, table, log, timeout, config)
  return [Step(pipeline, 0), pipeline]


def serialize(obj):
  return obj.step


class Step:
  def __init__(self, pipeline, step: int):
    self.pipeline = pipeline
    self.step = step

  def combine(self, params={}, config={}):
    return self.pipeline.combine(params, config)

  def run(self, application: str, params={}, output_format=None, config={}):
    return self.pipeline.run(application, params, output_format, config)


class Pipeline:
  def __init__(self, name: str, table: str, log: str, timeout: int, config: Dict[str, Any]):
    self.config = dict(config)
    self.functions = {}
    self.log = log
    if "memory_size" in self.config:
      self.memory_size = self.config["memory_size"]
      del self.config["memory_size"]
    else:
      self.memory_size = 1536

    if "thundra" in self.config:
      self.thundra = self.config["thundra"]
      del self.config["thundra"]
    else:
      self.thundra = False
    self.name = name
    self.pipeline = []
    self.table = table
    self.timeout = timeout
    self.formats = self.__get_formats__()

  def __add__(self, name, path, function_params, pipeline_params):
    self.functions[name] = function_params
    pipeline_params["name"] = name
    modules = self.__get_imports__(path)
    if len(self.pipeline) > 0:
      self.pipeline[-1]["output_function"] = name
    imports = modules.intersection(SUPPORTED_LIBRARIES)
    formats = modules.intersection(set(self.formats.keys()))
    for format in list(formats):
      formats = formats.union(self.formats[format])
    self.functions[name]["imports"] = list(imports)
    if "formats" in self.functions[name]:
      self.functions[name]["formats"] = self.functions[name]["formats"].union(formats)
    else:
      self.functions[name]["formats"] = formats

    if name in self.functions:
      assert(self.functions[name] == function_params)
    self.functions[name]["formats"] = list(self.functions[name]["formats"])
    self.pipeline.append(pipeline_params)

  def __get_formats__(self):
    folder = "{0:s}/formats/".format(currentdir)
    formats = {}
    files =  set()
    for file in os.listdir(folder):
      if file.endswith(".py"):
        format = file.split(".")[0]
        files.add(format)
    files.remove("iterator")

    for file in files:
      path = folder + file + ".py"
      imports  = self.__get_imports__(path)
      formats[file] = imports.intersection(files)

    return formats

  def __get_imports__(self, path):
    imports = set()
    if self.thundra:
      imports.add("thundra")
    with open(path, "r") as f:
      lines = f.readlines()
      for line in lines:
        for regex in IMPORT_REGEXES:
          m = regex.match(line)
          if m:
            imports.add(m.group(1).split(".")[0])
            break
    return imports

  def combine(self, params={}, config={}):
    output_format = self.functions[self.pipeline[-1]["name"]]["output_format"]
    name = "combine-{0:s}-files".format(output_format)
    function_params = {**{
      "file": "combine_files",
      "formats": set([output_format]),
      "memory_size": self.memory_size,
      "output_format": output_format
    }, **config}
    path = "{0:s}/formats/{1:s}.py".format(currentdir, output_format)
    self.__add__(name, path, function_params, params)
    return Step(self, len(self.pipeline))

  def compile(self, output_file):
    configuration = self.get_configuration(output_file)
    setup.setup(json.loads(json.dumps(configuration, default=serialize)))

  def get_configuration(self, output_file):
    configuration = {**{
      "bucket": self.table.replace("s3://", ""),
      "log": self.log.replace("s3://", ""),
      "timeout": self.timeout,
      "functions": self.functions,
      "pipeline": self.pipeline,
    }, **self.config}
    #print(json.dumps(configuration, indent=2, default=serialize))
    return configuration

  def run(self, name, params={}, output_format=None, config={}):
    function_params = {**{
      "application": name,
      "file": "application",
      "memory_size": self.memory_size,
      "output_format": output_format,
    }, **config}
    path = "{0:s}/applications/{1:s}.py".format(currentdir, name)
    self.__add__(name, path, function_params, params)
    return Step(self, len(self.pipeline))


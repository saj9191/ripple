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
import inspect
import json
import os
import re
from setup.lambda_setup import LambdaSetup
from setup.openwhisk_setup import OpenWhiskSetup
import sys
import util
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
from typing import Any, Dict, Optional


IMPORT_REGEXES = [re.compile(r"import ([a-zA-Z\_\.]+)"), re.compile(r"from ([a-zA-Z\_\.]+) import (.*)")]
SUPPORTED_LIBRARIES = set(["PIL", "numpy", "sklearn"])


def serialize(obj):
  return obj.step


class MapVariable:
  def __init__(self, name, params):
    self.name = name
    self.params = params

  def __getattr__(self, name):
    def method(*pargs, **kargs):
      # Method was called. This is the input key to the next stage.
      self.params[self.name] = "key"
      if kargs:
        if "params" in kargs:
          for key in kargs["params"]:
            if type(kargs["params"][key]) == MapVariable:
              self.params[kargs["params"][key].name] = key
    return method
    

class Step:
  def __init__(self, pipeline, step: int, format: Optional[str]=None):
    self.format = format
    self.pipeline = pipeline
    self.step = step

  def combine(self, params={}, config={}):
    return self.pipeline.combine(self.format, params, config)

  def map(self, table, func, params={},  config={}):
    return self.pipeline.map(table, func, self.format, params, config)

  def run(self, application: str, params={}, output_format=None, config={}):
    return self.pipeline.run(application, self.format, output_format, params, config)

  def sort(self, identifier, params={}, config={}):
    return self.pipeline.sort(identifier, self.format, params, config)

  def split(self, params={}, config={}):
    return self.pipeline.split(self.format, params, config)

  def top(self, identifier: str, number: int, params={}, config={}):
    return self.pipeline.top(identifier, number, self.format, params, config)

class Pipeline:
  def __init__(self, name: str, table: str, log: str, timeout: int, config: Dict[str, Any]):
    self.config = dict(config)
    self.functions = {}
    self.log = log
    if "provider" in self.config:
      self.provider = self.config["provider"]
      del self.config["provider"]
    else:
      self.provider = "lambda"

    if "memory_size" in self.config:
      self.memory_size = self.config["memory_size"]
      del self.config["memory_size"]
    else:
      self.memory_size = 1536

    self.name = name
    self.pipeline = []
    self.table = table
    self.timeout = timeout
    self.formats = self.__get_formats__()

  def input(self, format):
    assert(len(self.pipeline) == 0)
    return Step(self, 0, format)

  def __add__(self, name, input_format, function_params, pipeline_params, path: Optional[str]=None):
    function_params["provider"] = self.provider
    self.functions[name] = function_params
    pipeline_params["name"] = name
    if len(self.pipeline) > 0:
      self.pipeline[-1]["output_function"] = name

    formats = set([input_format])
    if "formats" in self.functions[name]:
      formats = formats.union(self.functions[name]["formats"])

    formats = formats.intersection(set(self.formats.keys()))
    imports = set()
    if path:
      modules = self.__get_imports__(path)
      imports = modules.intersection(SUPPORTED_LIBRARIES)
      formats = formats.union(modules.intersection(set(self.formats.keys())))

    for format in list(formats):
      formats = formats.union(self.formats[format])
    self.functions[name]["formats"] = list(formats)
    self.functions[name]["imports"] = list(imports)

    if name in self.functions:
      assert(self.functions[name] == function_params)
    self.pipeline.append(dict(pipeline_params))

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
    with open(path, "r") as f:
      lines = f.readlines()
      for line in lines:
        for regex in IMPORT_REGEXES:
          m = regex.match(line)
          if m:
            # TODO: Figure out if there's a better way to handle imports
            # Currently have to special case things like from formats import new_line
            format = m.group(1).split(".")[0]
            if format == "formats":
              format = m.group(2)
            imports.add(format)
            break
    return imports

  def combine(self, output_format, params={}, config={}):
    name = "combine-{0:s}-files".format(output_format)
    function_params = {**{
      "file": "combine_files",
      "formats": set([output_format]),
      "memory_size": self.memory_size,
      "output_format": output_format
    }, **config}
    print("params", params)
    print("config", config)
    self.__add__(name, output_format, function_params, params, None)
    return Step(self, len(self.pipeline), output_format)

  def compile(self, output_file, dry_run=False):
    configuration = self.get_configuration(output_file)
    jconfig = json.dumps(configuration, default=serialize, indent=2)
    if dry_run:
      print(jconfig)
    else:
      params = json.loads(jconfig)
      if self.provider == "openwhisk":
        s = OpenWhiskSetup(params)
      else:
        s = LambdaSetup(params)
      s.start()

  def get_configuration(self, output_file):
    output_file = os.path.dirname(os.path.realpath(__file__)) + "/" + output_file
    configuration = {**{
      "bucket": self.table.replace("s3://", ""),
      "log": self.log.replace("s3://", ""),
      "timeout": self.timeout,
      "functions": self.functions,
      "pipeline": self.pipeline,
    }, **self.config}

    with open(output_file, "w+") as f:
      f.write(json.dumps(configuration, indent=2, default=serialize))
    return configuration

  def map(self, table, func, input_format, params={}, config={}):
    input = MapVariable("input_key_value", params)
    bucket = MapVariable("bucket_key_value", params)
    func(input, bucket)

    name = "map"
    self.__add__(name, input_format, config, params, None)

    if params["input_key_value"] == "key":
      input = Step(self, len(self.pipeline), input_format)
      bucket = params["bucket_key_value"]
    else:
      # TODO: For now assume the bucket key has the same format
      # as the input key
      input = params["input_key_value"]
      bucket = Step(self, len(self.pipeline), input_format)
    step = func(input, bucket)

    if params["input_key_value"] != "key":
      del self.pipeline[-1][params["input_key_value"]]
    else:
      del self.pipeline[-1][params["bucket_key_value"]]
    return step

  def partition(self, identifier, input_format, num_bins, params={}, config={}):
    name = "pivot_file"
    function_params = {**{
      "file": name,
      "identifier": identifier,
      "input_format": input_format,
      "memory_size": self.memory_size,
      "num_pivot_bins": num_bins
    }, **config}
    self.__add__(name, input_format, function_params, params, None)
    return Step(self, len(self.pipeline), format)

  def run(self, name, input_format, output_format=None, params={}, config={}):
    function_params = {**{
      "application": name,
      "file": "application",
      "input_format": input_format,
      "memory_size": self.memory_size,
      "output_format": output_format,
    }, **config}
    path = "{0:s}/applications/{1:s}.py".format(currentdir, name)
    self.__add__(name, input_format, function_params, params, path)
    return Step(self, len(self.pipeline), output_format)

  def sort(self, identifier: str, input_format, params={}, config={}):
    step = self.split(input_format, dict(params), dict(config))
    split_size = None
    if "split_size" in params:
      split_size = params["split_size"]
      del params["split_size"]
    num_bins = params["num_bins"]
    del params["num_bins"]    
    step = self.partition(identifier, input_format, num_bins, dict(params), dict(config))
    self.combine("pivot", {**params, **{"sort": True, "num_pivot_bins": num_bins}}, dict(config))
    extra_params = {"ranges": True}
    if split_size is not None:
      extra_params["split_size"] = split_size
    self.split(input_format, {**params, **extra_params}, dict(config))
    name = "sort"
    function_params = {**{
      "file": name,
      "identifier": identifier,
      "input_format": input_format,
      "memory_size": self.memory_size,
    }, **config}
    self.__add__(name, input_format, function_params, params, None)
    self.combine(input_format, params, config)
    return Step(self, len(self.pipeline), input_format)

  def split(self, input_format, params={}, config={}):
    name = "split_file"
    function_params = {**{
      "file": name,
      "memory_size": self.memory_size,
      "split_size": 100*1000*1000,
    }, **config}
    p = dict(params)
    if "num_bins" in p:
      del p["num_bins"]
    self.__add__(name, input_format, function_params, p, None)
    return Step(self, len(self.pipeline), format)

  def top(self, identifier, number: int, input_format, params={}, config={}):
    name = "top"
    function_params = {**{
      "file": name,
      "identifier": identifier,
      "input_format": input_format,
      "memory_size": self.memory_size,
      "split_size": 100*1000*1000,
    }, **config}

    params = {**{
      "number": number,
    }, **params}
    self.__add__(name, input_format, function_params, params, None)
    return Step(self, len(self.pipeline), format)



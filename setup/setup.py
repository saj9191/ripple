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

import abc
import argparse
import boto3
import botocore
import json
import os
import shutil
import subprocess
import util


class Setup:
  def __init__(self, params):
    self.params = params

  @abc.abstractmethod
  def __add_additional_files__(self, zip_directory):
    raise Exception("Setup::__add_additional_files__ not implemented")

  def __create_parameter_files__(self, zip_directory, function_name):
    for i in range(len(self.params["pipeline"])):
      pparams = self.params["pipeline"][i]
      if pparams["name"] == function_name:
        p = {**self.params["functions"][function_name], **pparams}
        for value in ["timeout", "num_bins", "bucket", "storage_class", "log", "scheduler"]:
          if value in self.params:
            p[value] = self.params[value]

        name = "{0:d}.json".format(i)
        json_path = "{0:s}/{1:s}".format(zip_directory, name)
        f = open(json_path, "w")
        f.write(json.dumps(p))
        f.close()

  def __copy_file__(self, directory, file_path):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    index = file_path.rfind("/")
    file_name = file_path[index + 1:]
    shutil.copyfile(dir_path + "/" + file_path, directory + "/" + file_name)
    return file_name

  # Creates a table / bucket to load data to.
  @abc.abstractmethod
  def __create_table__(self, name):
    raise Exception("Setup::__create_table__ not implemented")

  # Returns a list of function names currently uploaded to provider.
  @abc.abstractmethod
  def __get_functions__(self):
    raise Exception("Setup::__create_table__ not implemented")

  def __make_directory__(self, zip_directory, name):
    dest = zip_directory + "/" + name
    if not os.path.isdir(dest):
      os.mkdir(dest)
    open(dest + "/__init__.py", "a").close()


  # Setup the account credientials
  @abc.abstractmethod
  def __setup_credentials__(self):
    raise Exception("Setup::__setup_credentials__ not implemented")

  def __setup_function__(self, name, create):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_directory = dir_path + "/" + self.params["region"]
    zip_directory = parent_directory + "/lambda_dependencies"
    zip_file = "lambda.zip"

    if os.path.isdir(zip_directory):
      shutil.rmtree(zip_directory)

    function_params = self.params["functions"][name]
    os.makedirs(zip_directory)

    self.__zip_ripple_file__(zip_directory, function_params)
    self.__zip_application__(zip_directory, function_params)
    self.__zip_formats__(zip_directory, function_params)
    self.__create_parameter_files__(zip_directory, name)
    self.__add_additional_files__(zip_directory)
    os.chdir(zip_directory)
    subprocess.call("zip -q -r9 ../{0:s} .".format(zip_file), shell=True)
    os.chdir("..")
    self.__upload_function__(name, os.getcwd() + "/" + zip_file, function_params, create)
    os.chdir("..")
    shutil.rmtree(zip_directory)

  def __setup_functions__(self):
    functions = self.__get_functions__()
    for name in self.params["functions"]:
      self.__setup_function__(name, name not in functions) 

  # Setup serverless triggers on the table
  @abc.abstractmethod
  def __setup_table_notifications__(table_name):
    raise Exception("Setup::__setup_table_notifications__ not implemented")

  # Creates the user role with the necessary permissions to execute the pipeline. 
  # This includes function and table permissions.
  @abc.abstractmethod
  def __setup_user_permissions__(self):
    raise Exception("Setup::__setup_user_permissions__ not implemented")

  # Uploads the code for the function and sets up the triggers.
  @abc.abstractmethod
  def __upload_function__(self, name, zip_file, create):
    raise Exception("Setup::__upload_function__ not implemented")

  def __zip_application__(self, zip_directory, fparams):
    if "application" in fparams:
      dest = zip_directory + "/applications"
      self.__make_directory__(zip_directory, "applications")
      self.__copy_file__(dest, "../applications/__init__.py")
      self.__copy_file__(dest, "../applications/{0:s}.py".format(fparams["application"]))

  def __zip_formats__(self, zip_directory, fparams):
    dest = zip_directory + "/formats"
    self.__make_directory__(zip_directory, "formats")
    for file in ["../formats/iterator.py", "../formats/pivot.py"]:
      self.__copy_file__(dest, file)

    if "formats" in fparams:
      for format in fparams["formats"]:
        self.__copy_file__(dest, "../formats/{0:s}.py".format(format))

  def __zip_ripple_file__(self, zip_directory, fparams):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file = "{0:s}.py".format(fparams["file"])
    if fparams["provider"] == "lambda":
      dest = "/main.py"
    else:
      dest = "/__main__.py"
    shutil.copyfile(dir_path + "/../lambdas/{0:s}".format(file), zip_directory + dest)

    dest = zip_directory + "/database"
    self.__make_directory__(zip_directory, "database")
    for file in ["database", "s3"]:
      self.__copy_file__(dest, "../database/{0:s}.py".format(file))
 
    self.__copy_file__(zip_directory, "../util.py")
    self.__copy_file__(zip_directory, "../__init__.py")

  def start(self):
    self.__setup_credentials__()
    self.__setup_user_permissions__()
    self.__create_table__(self.params["bucket"])
    self.__create_table__(self.params["log"])
    self.__setup_functions__()
    self.__setup_table_notifications__(self.params["bucket"])


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  #setup(params)


if __name__ == "__main__":
  main()

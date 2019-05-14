import os
import requests
import shutil
import subprocess
from setup.setup import Setup


class OpenWhiskSetup(Setup):
  def __init__(self, params):
    Setup.__init__(self, params)

  def __add_additional_files__(self, zip_directory):
    src = os.path.expanduser("~") + "/.aws/"
    dest = zip_directory + "/aws/"
    os.mkdir(dest)
    for file in ["credentials", "config"]:
      shutil.copyfile(src + file, dest + file)

  def __create_table__(self, name):
    pass

  def __get_functions__(self):
    output = subprocess.check_output("wsk action list --insecure", shell=True).decode("utf-8").strip()
    lines = output.split("\n")[1:]
    functions = set(list(map(lambda line: line.split(" ")[0], lines)))
    return functions

  def __setup_credentials__(self):
    try:
      self.auth_key = subprocess.check_output("wsk property get --auth --insecure", shell=True).decode("utf-8").split()[2]
      username, password = self.auth_key.split(":")
    except Exception as e:
      print(e)
      username = input("Please specify API HOST:").strip()
      password = input("Please specify AUTH KEY:").strip()
      subprocess.check_output("wsk property set --apihost {0:s} --auth {1:s} --insecure".format(username, password), shell=True)

    self.username = username
    self.password = password

  def __setup_table_notifications__(self, table_name):
    pass

  def __setup_user_permissions__(self):
    pass

  def __upload_function__(self, name, zip_file, function_params, create):
    subprocess.check_output("wsk action update {0:s} --kind python:3 {1:s} --insecure".format(name, zip_file), shell=True)

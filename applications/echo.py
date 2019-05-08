import shutil
import util
from database.database import Database
from typing import List

def run(database: Database, file: str, params, input_format, output_format):
  output_file = "/tmp/" + util.file_name(output_format)
  print("Echo!")
  shutil.move(file, output_file)
  return [output_file]

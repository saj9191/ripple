import os
import shutil
import subprocess
import util
from database.database import Database
from typing import List


def run(database: Database, file: str, params, input_format, output_format):
  database.download(params["database_bucket"], "crux", "/tmp/crux")

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  output_dir = "/tmp/confidence-crux-output-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    "--output-dir", output_dir,
  ]

  command = "cd /tmp; ./crux assign-confidence {0:s} {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  output_files = []
  input_file = "{0:s}/assign-confidence.target.txt".format(output_dir)
  output_format["ext"] = "confidence"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  os.rename(input_file, output_file)
  output_files.append(output_file)

  return output_files

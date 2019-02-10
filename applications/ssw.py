import subprocess
import util
from database import Database
from typing import List


def run(database: Database, file: str, params, input_format, output_format, offsets: List[int]):
  with open("/tmp/ssw_test", "wb+") as f:
    database.download(params["program_bucket"], "ssw_test", f)

  subprocess.call("chmod 755 /tmp/ssw_test", shell=True)

  target_fasta = params["target"]
  output_format["ext"] = "blast"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))

  with open("/tmp/{0:s}".format(target_fasta), "wb+") as f:
    database.download(params["target_bucket"], target_fasta, f)

  command = "cd /tmp; ./ssw_test -p {0:s} {1:s} > {2:s}".format(target_fasta, file, output_file)

  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  return [output_file]

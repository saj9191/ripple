import subprocess
import util
import os
from database import Database
from typing import List

def run(database: Database, file: str, params, input_format, output_format, offsets: List[int]):
    dir_path = "/tmp/fastore_test/"
    bucket = params["bucket"]
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    for entry in database.get_entries(params["program_bucket"]):
      with open(dir_path + "/" + entry.key) as f:
        entry.download(f)

    subprocess.call("chmod 755 /tmp/fastore_test",shell=True)

    input_file = file
    output_file = "/tmp/{0:s}".format(util.file_name(output_format))
    arguments = [
        "in " + input_file,
        "pair " + input_file,
        "out " + os.path.join(output_file, "OUTPUT"),
        # "threads 2"
    ]
    output_file = "/tmp/{0:s}".format(util.file_name(output_format))

    command = "cd /tmp; ./fastore_compress.sh --lossless --{0:s} > {2:s}".format(" --".join(arguments),output_file)
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    return[output_file]




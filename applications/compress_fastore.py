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
      with open(dir_path + entry.key,"wb+") as f:
        entry.download(f)


    for (root, dirs, files) in os.walk(dir_path, topdown=True):
        for ele in files:
            ele = ''.join(ele)
            subprocess.call("chmod 755 "+ dir_path +ele,shell = True)


    input_file = file
    tmp_file = "/tmp/{0:s}".format(util.file_name(output_format)).split("/")[:-1]
    output_path = "/".join(tmp_file)
    output_file = os.path.join(output_path, "1-1-1-output")
    print("output",output_file)
    

    arguments = [
        "in " + input_file,
        "pair " + input_file,
        "out " + output_file
    ]

    command = "cd /tmp/fastore_test; ./fastore_compress.sh --lossless --{0:s}".format(" --".join(arguments),output_file)
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)

    output_list = []
    for (root, dirs, files) in os.walk(output_path,topdown = True):
        for file in files:
            file = "".join(file)
            if file.endswith(".cmeta") or file.endswith(".cdata"):
                output_list.append(os.path.join(output_path, file))

    print("outputlist",output_list)

    return output_list




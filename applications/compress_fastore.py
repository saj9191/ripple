import os
import subprocess
import util
import boto3


def run(file, params, input_format, output_format, offsets):
    if params["action"] == "decompress":
        assert ("ArInt" in file)
        for f in ["outfileChrom", "outfileName"]:
            input_format["suffix"] = f
            file_path = util.download(params["bucket"], util.file_name(input_format))
            os.rename(file_path, "/tmp/test_{0:s}-0".format(f))
        input_name = "/tmp/test_outfileArInt-0"
    else:
        input_name = file
    # why to rename, how to get access to the specific file to compress
    os.rename(file, input_name)

    print("Download compress shell script")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(params["program_bucket"])
    for object in bucket.objects.all():
        util.download(params["program_bucket"], object.key)
        output_dir = "/tmp/fastore-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    if params["action"] == "compress":
        output_dir = os.path.join(output_dir, "compressed_input")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    # Execute Shell Script and Parameter configuration
    arguments = [
        "in " + input_name,
        "pair " + input_name,
        "out " + os.path.join(output_dir, "OUTPUT"),
        # "threads 2"
    ]

    command = "cd /tmp; ./fastore_compress.sh --lossless --{0:s}".format(" --".join(arguments))
    print("Compress command:" + command)
    util.check_output(command)
    # output files
    output_files = []
    # if params["action"] == "decompress":
    result_dir = output_dir
    # else:
    # result_dir = "{0:s}/compressed_input".format(output_dir)
    # what is output format
    for subdir, dirs, files in os.walk(result_dir):
        if params["action"] == "decompressed":
            assert (len(files) == 1)

        for f in files:
            if params["action"] == "compress":
                output_format["suffix"] = f.split("_")[-1].split("-")[0]
            else:
                output_format["suffix"] = "decompressed"

            output_file = "/tmp/{0:s}".format(util.file_name(output_format))
            # rename the output-file
            # if params["action"] == "compress":
            # os.rename("{0:s}/{1:s}".format(output_dir, f), output_file)
            # else:
            os.rename("{0:s}/{1:s}".format(output_dir, f), output_file)
            output_files.append(output_file)
                #print("output files")
                #print(output_files)
    return output_files




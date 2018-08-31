import boto3
import subprocess
import util


def run(file, params, input_format, output_format):
  s3 = boto3.resource("s3")
  program_bucket = s3.Bucket(params["program_bucket"])

  with open("/tmp/ssw_test", "wb") as f:
    program_bucket.download_fileobj("ssw_test", f)

  subprocess.call("chmod 755 /tmp/ssw_test", shell=True)

  target_bucket = s3.Bucket(params["target_bucket"])
  target_fasta = params["target"]
  output_format["ext"] = "blast"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))

  with open("/tmp/{0:s}".format(target_fasta), "wb") as f:
    target_bucket.download_fileobj(target_fasta, f)

  command = "cd /tmp; ./ssw_test -p {0:s} {1:s} > {2:s}".format(target_fasta, file, output_file)
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  return [output_file]

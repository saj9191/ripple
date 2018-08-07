import boto3
import subprocess
import util


def run(query_fasta, params, m):
  s3 = boto3.resource("s3")
  program_bucket = s3.Bucket(params["program_bucket"])

  with open("/tmp/ssw_test", "wb") as f:
    program_bucket.download_fileobj("ssw_test", f)

  subprocess.call("chmod 755 /tmp/ssw_test", shell=True)

  target_bucket = s3.Bucket(params["extra_params"]["target_bucket"])
  target_fasta = params["extra_params"]["target_file"]
  file_id = int(target_fasta.split("-")[-1])
  m["prefix"] = "ssw"
  m["file_id"] = file_id
  util.print_request(m, params)
  num_files = sum(1 for _ in target_bucket.objects.all())
  m["last"] = (file_id == num_files)
  m["ext"] = "blast"
  output_file = "/tmp/{0:s}".format(util.file_name(m))

  with open("/tmp/{0:s}".format(target_fasta), "wb") as f:
    target_bucket.download_fileobj(target_fasta, f)

  command = "cd /tmp; ./ssw_test -p {0:s} {1:s} > {2:s}".format(target_fasta, query_fasta, output_file)
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  return [output_file]

import boto3
import json
import re.compile("combined-spectra-([0-9\.]+)-([0-9]+).txt")

def run_percolator(bucket_name, spectra_file, max_train):
  util.clear_tmp()
  m = INPUT_FILE.match(spectra_file)
  ts = m.group(1)
  num_files = int(m.group(2))
  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket("maccoss-human-fasta")
  spectra_bucket = s3.Bucket(bucket_name)

  with open("/tmp/{0:s}".format(spectra_file), "wb") as f:
    spectra_bucket.download_fileobj(spectra_file, f)

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)

  output_dir = "/tmp/percolator-crux-output-{0:s}".format(m.group(1))

  arguments = [
    "--subset-max-train", max_train,
    "--quick-validation", "T",
    "--output-dir", output_dir
  ]

  command = "cd /tmp; ./crux percolator {0:s} {1:s}".format(spectra_file, " ".join(arguments))
  subprocess.call(command, shell=True)

  done = False
  while not done:
    process_output = str(subprocess.check_output("ps aux | grep crux", shell=True))
    done = len(process_output.split("
")) == 1
    time.sleep(1)

  for item in ["target.psms", "decoy.psms", "target.peptides", "decoy.peptides"]:
    s3.Object(bucket_name, "{0:s}-{1:s}-{2:d}.txt".format(item, ts, num_files)).put(Body=open("{0:s}/percolator.{1:s}.txt".format(output_dir, item), 'rb'))

def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  spectra_file = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("percolator.json").read())
  run_percolator(bucket_name, spectra_file, params["max_train"])

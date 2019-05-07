import boto3
import os
import sys
sys.path.append("../tests")
import pipeline


def identify_fasta_type(file_name, key, f):
  task_queue = run(file_name, "../scripts/tide/species.json", ["crux"])
  assert(task_queue.qsize() == 1)
  payload = task_queue.get()
  bucket = payload["Records"][0]["s3"]["extra_params"]["map_bucket"]
  f.write("{0:s} {1:s}\n".format(key, bucket))

def run(file_name, configuration, files):
  pp = pipeline.Pipeline(configuration)
  pp.populate_table("maccoss-fasta", "tide/", files)

  name = "0/123.400000-13/1-1/1-0.000000-1-tide.mzML"
  pp.run(name, file_name)
  return pp.task_queue


def test_all(output_file):
  s3 = boto3.resource("s3")
  bucket_name = "tide-source-data"
  bucket = s3.Bucket(bucket_name)

  for obj in bucket.objects.all():
    print("Testing", obj.key)
    file_name = "../scripts/tide/{0:s}".format(obj.key.split("/")[-1])
    with open(file_name, "wb+") as g:
      s3.Object(bucket_name, obj.key).download_fileobj(g)
    with open(output_file, "a+") as f:
      identify_fasta_type(file_name, obj.key, f)
    os.remove(file_name)

test_all("tide/output_file.txt")

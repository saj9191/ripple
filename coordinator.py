import argparse
import boto3
import split
import subprocess
import time
import util


def combine_analyze(bucket_prefix, timestamp, nonce, num_splits):
  s3 = boto3.resource("s3")
  bucket_name = "{0:s}-human-analyze-spectra".format(bucket_prefix)
  bucket = s3.Bucket(bucket_name)
  processed = set()

  ts = "{0:f}".format(timestamp)
  file_name = "combined-spectra-{0:s}-{1:d}.txt".format(ts, nonce)
  f = open(file_name, "w+")

  while len(processed) < num_splits:
    for obj in bucket.objects.all():
      if ts in obj.key and obj.key not in processed:
        spectra = s3.Object(bucket_name, obj.key).get()["Body"].read().decode("utf-8")
        if len(processed) == 0:
          f.write(spectra)
        else:
          results = spectra.split("\n")[1:]
          f.write("\n".join(results))
        processed.add(obj.key)
    time.sleep(5)

  return file_name


def run_percolator(bucket_prefix, timestamp, nonce, input_file):
  arguments = [
    "--quick-validation", "T",
    "--overwrite", "T",
  ]

  subprocess.call("sudo ./crux percolator {0:s} {1:s}".format(input_file, " ".join(arguments)), shell=True)
  bucket_name = "{0:s}-human-output-spectra".format(bucket_prefix)
  for item in ["peptides", "psms"]:
    input_file = "percolator.target.{0:s}.txt".format(item)
    output_file = "percolator.target.{0:s}.{1:f}.{2:d}.txt".format(item, timestamp, nonce)
    subprocess.call("s3cmd put crux-output/{0:s} s3://{1:s}/{2:s}".format(input_file, bucket_name, output_file), shell=True)


def run(args):
  bucket_name = "{0:s}-human-input-spectra".format(args.bucket_prefix)
  m = util.parse_file_name(args.file)
  timestamp = m["timestamp"]
  nonce = m["nonce"]

  start_time = time.time()
  num_splits = split.split_spectra(args.file, bucket_name, args.batch_size, args.chunk_size)
  end_time = time.time()
  print("SPLIT DURATION", end_time - start_time)

  start_time = time.time()
  combine_file = combine_analyze(args.bucket_prefix, timestamp, nonce, num_splits)
  end_time = time.time()
  print("COMBINE DURATION", end_time - start_time)

  start_time = time.time()
  run_percolator(args.bucket_prefix, timestamp, nonce, combine_file)
  end_time = time.time()
  print("PERCOLATOR DURATION", end_time - start_time)


def main():
  start_time = time.time()
  parser = argparse.ArgumentParser()
  parser.add_argument('--file', type=str, required=True, help="File to analyze")
  parser.add_argument('--bucket_prefix', type=str, required=True, help="Name of bucket prefix")
  parser.add_argument('--batch_size', type=int, required=True, help="Spectra batch sizes")
  parser.add_argument('--chunk_size', type=int, required=True, help="Number of bytes to read from S3 at once")
  args = parser.parse_args()
  run(args)
  end_time = time.time()
  print("TOTAL DURATION", end_time - start_time)


if __name__ == "__main__":
  main()

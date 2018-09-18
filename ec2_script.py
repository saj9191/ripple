import argparse
import boto3
import heapq
import mzML
import os
import queue
import shutil
import subprocess
import threading
import time
import util

class SpeciesRequest(threading.Thread):
  def __init__(self, thread_id, file_name, species, queue):
    assert(file_name is not None)
    super(SpeciesRequest, self).__init__()
    self.file_name = file_name
    self.species = species
    self.thread_id = thread_id
    self.queue = queue

  def count(self, perc_dir):
    lines = open("{0:s}/assign-confidence.target.txt".format(perc_dir)).readlines()[1:]
    count = 0
    for line in lines:
      parts = line.split("\t")
      value = float(parts[9])
      if value <= 0.01:
        count += 1
    return count

  def run(self):
    fasta_dir = "{0:s}-index".format(self.species)
    if not os.path.isdir(fasta_dir):
      os.mkdir(fasta_dir)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket("shjoyner-fasta")
    for item in ["auxlocs", "pepix", "protix"]:
      with open("{0:s}/{1:s}".format(fasta_dir, item), "wb") as f:
        bucket.download_fileobj("{0:s}/{1:s}".format(self.species, item), f)

    tide_dir = tide(self.species, fasta_dir, self.file_name)
    perc_dir = confidence(self.species, tide_dir)
    self.queue.put([self.species, self.count(perc_dir)])
    shutil.rmtree(tide_dir)
    shutil.rmtree(perc_dir)


def s3():
  [access_key, secret_key] = util.get_credentials("default")
  session = boto3.Session(
           aws_access_key_id=access_key,
           aws_secret_access_key=secret_key,
           region_name="us-west-2"
  )
  s3 = session.resource("s3")
  return s3


def create_top_file(file_name):
  obj = s3().Object("maccoss-ec2", file_name)
  top_name = "top-{0:s}".format(file_name)
  it = mzML.Iterator(obj, {}, 4000, 10*1000*1000)

  top = []
  more = True
  while more:
    [spectra, more] = it.next(identifier="tic")

    for spectrum in spectra:
      heapq.heappush(top, spectrum)
      if len(top) > 1000:
        heapq.heappop(top)

  offsets = {
    "header": {
      "start": 0,
      "end": it.header_length
    }
  }
  content = mzML.Iterator.fromArray(obj, list(map(lambda t: t[1], top)), offsets)
  with open(top_name, "w+") as f:
    f.write(content)
  return top_name


def tide(species, fasta, file_name):
  output_dir = "{0:s}-tide-output".format(species)
  arguments = [
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
    "--overwrite", "T",
  ]
  cmd = "./crux tide-search {0:s} {1:s} {2:s} > tmp".format(file_name, fasta, " ".join(arguments))
  subprocess.call(cmd, shell=True)
  return output_dir


def confidence(species, tide_dir):
  output_dir = "{0:s}-conf-output".format(species)
  arguments = [
    "--output-dir", output_dir
  ]

  file_name = "{0:s}/tide-search.txt".format(tide_dir)
  subprocess.call("./crux assign-confidence {0:s} {1:s} > tmp".format(file_name, " ".join(arguments)), shell=True)
  return output_dir


def percolator(species, tide_dir):
  start_time = time.time()
  output_dir = "{0:s}-perc-output".format(species)
  arguments = [
    "--quick-validation", "T",
    "--overwrite", "T",
    "--output-dir", output_dir
  ]

  file_name = "{0:s}/tide-search.txt".format(tide_dir)

  subprocess.call("./crux percolator {0:s} {1:s} > tmp".format(file_name, " ".join(arguments)), shell=True)
  end_time = time.time()
  print("{0:f} PERCOLATOR DURATION: {1:f}".format(time.time(), end_time - start_time))
  return output_dir


def identify_species(file_name):
  start_time = time.time()
  bucket = s3().Bucket("shjoyner-fasta")
  threads = []

  top_file_name = create_top_file(file_name)
  q = queue.Queue()

  species = set()
  for obj in bucket.objects.all():
    if "/" in obj.key:
      species.add(obj.key.split("/")[-2])

  for specie in species:
    thread = SpeciesRequest(len(threads), top_file_name, specie, q)
    thread.start()
    threads.append(thread)

  for thread in threads:
    thread.join()

  best_species = None
  best_count = 0
  while not q.empty():
    [species, count] = q.get()
    if count > best_count or best_species is None:
      best_count = count
      best_species = species
    else:
      shutil.rmtree("{0:s}-index".format(species))

  end_time = time.time()
  print("{0:f} IDENTIFY DURATION: {1:f}".format(time.time(), end_time - start_time))
  return best_species


def download_input(file_name, bucket):
  start_time = time.time()
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket)
  with open(file_name, "wb") as f:
    bucket.download_fileobj(file_name, f)
  end_time = time.time()
  print("{0:f} DOWNLOAD DURATION: {1:f}".format(time.time(), end_time - start_time))


def upload_output(file_name, bucket):
  start_time = time.time()
  client = boto3.client("s3")
  tc = boto3.s3.transfer.TransferConfig()
  t = boto3.s3.transfer.S3Transfer(client=client, config=tc)
  t.upload_file(file_name, bucket, file_name)
  end_time = time.time()
  print("{0:f} UPLOAD DURATION: {1:f}".format(time.time(), end_time - start_time))


def run_tide(file_name, bucket):
  start_time = time.time()
  if os.path.isfile("TN_CSF_062617_01.mzML"):
    os.remove("TN_CSF_062617_01.mzML")
  download_input(file_name, bucket)
  #species = identify_species(file_name)
  species = "normalHuman"
  fasta_dir = "{0:s}-index".format(species)
  st = time.time()
  tide_dir = tide(species, fasta_dir, file_name)
  et = time.time()
  print("{0:f} TIDE DURATION: {1:f}".format(time.time(), et - st))
  upload_output("{0:s}/tide-search.txt".format(tide_dir), bucket)
  end_time = time.time()
  print("{0:f} TOTAL DURATION: {1:f}".format(time.time(), end_time - start_time))


def ssw_test(file_name):
  start_time = time.time()
  output_file = "output.txt"
  cmd = "./ssw_test -p uniprot-all.fasta {0:s} > {1:s}".format(file_name, output_file)
  subprocess.call(cmd, shell=True)
  end_time = time.time()
  print("{0:f} SSW DURATION: {1:f}".format(time.time(), end_time - start_time))
  return output_file


def run_ssw(file_name, bucket):
  start_time = time.time()
  download_input(file_name, bucket)
  output_file = ssw_test(file_name)
  upload_output(output_file, bucket)
  end_time = time.time()
  print("{0:f} TOTAL DURATION: {1:f}".format(time.time(), end_time - start_time))


def run_methyl(file_name, bucket):
  s3 = boto3.resource("s3")
  start_time = time.time()
  download_input(file_name, "maccoss-methyl-data")
  input_name = "/tmp/input"
  output_dir = "compressed"
  os.rename(file_name, input_name)
  cmd = "./output compress {0:s} {1:s}".format(input_name, output_dir)
  st = time.time()
  subprocess.call(cmd, shell=True)
  et = time.time()
  print("{0:f} COMPRESS DURATION: {1:f}".format(time.time(), et - st))

  st = time.time()
  compressed_dir = "{0:s}/compressed_input".format(output_dir)

  decompress_input = None
  for subdir, dirs, files in os.walk(compressed_dir):
    for f in files:
      if "ArInt" in f:
        decompress_input = f
      file_name = "{0:s}/{1:s}".format(compressed_dir, f)
      s3.Object(bucket, f).put(Body=open(file_name, "rb"))
  et = time.time()
  print("{0:f} CUPLOAD DURATION: {1:f}".format(time.time(), et - st))

  output_dir = "decompressed"
  cmd = "./output decompress {0:s}/{1:s} {2:s}".format(compressed_dir, decompress_input, output_dir)
  st = time.time()
  subprocess.call(cmd, shell=True)
  et = time.time()
  print("{0:f} DECOMPRESS DURATION: {1:f}".format(time.time(), et - st))
  upload_output("{0:s}/reconstructed_input-0".format(output_dir), bucket)
  end_time = time.time()
  print("{0:f} METHYL DURATION: {1:f}".format(time.time(), end_time - start_time))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--file', type=str, required=True, help="File to analyze")
  parser.add_argument('--bucket', type=str, required=True, help="Bucket to read and write to")
  parser.add_argument('--application', type=str, required=True, help="Application to run")
  args = parser.parse_args()

  if args.application == "tide":
    run_tide(args.file, args.bucket)
  elif args.application == "ssw":
    run_ssw(args.file, args.bucket)
  elif args.application == "methyl":
    run_methyl(args.file, args.bucket)


if __name__ == "__main__":
  main()

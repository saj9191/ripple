import argparse
import boto3
import inspect
import os
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import mzML


def create_spectra_file(num_spectra):
  spectra = []
  s3 = boto3.resource("s3")
  bucket_name = "tide-source-data"
  bucket = s3.Bucket(bucket_name)
  objs = bucket.objects.filter(Prefix="Coon-HeLa-APD-Data")

  for obj in objs:
    if len(spectra) < num_spectra:
      it = mzML.Iterator(s3.Object(bucket_name, obj.key), 1000*1000, s3=s3)
      more = True
      while len(spectra) < num_spectra and more:
        [s, more] = it.next()
        add_count = min(len(s), num_spectra - len(spectra))
        spectra += s[:add_count]
    else:
      break

  with open("../data/ASH_{0:d}.mzML".format(num_spectra), "w+") as f:
    it.from_array(obj, spectra, None, it.header, f)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--num_spectra", type=int, required=True, help="Number of spectra in file")
  args = parser.parse_args()
  create_spectra_file(args.num_spectra)


if __name__ == "__main__":
  main()

import argparse
import boto3
from database.s3 import S3
import inspect
import os
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import mzML


def create_spectra_file(name, num_spectra):
  spectra = []
#  name = "10sep2013_yeast_control_1.mzML"
  s3 = S3({})
  bucket_name = "tide-source-data"
  objs = s3.get_entries(bucket_name, "Coon-SingleShotFusionYeast/" + name)

  for obj in objs:
    if len(spectra) < num_spectra:
      it = mzML.Iterator(obj)
      extra = it.get_extra()
      more = True
      while len(spectra) < num_spectra and more:
        [items, _, more] = it.next()
        items = list(items)
        print(len(spectra), len(items))
        add_count = min(len(items), num_spectra - len(spectra))
        spectra += items[:add_count]
    else:
      break

  with open(name, "wb+") as f:
    it.from_array(spectra, f, extra)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--name", type=str, required=True, help="Name of input file")
  parser.add_argument("--num_spectra", type=int, required=True, help="Number of spectra in file")
  args = parser.parse_args()
  create_spectra_file(args.name, args.num_spectra)


if __name__ == "__main__":
  main()

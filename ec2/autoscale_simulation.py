import argparse
import boto3
import inspect
import json
import master
import os
import shutil
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import simulation


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--s3_application_url", type=str, required=True, help="S3 URL for application code")
  parser.add_argument("--parameters", type=str, required=True, help="File containing simulation distribution paramters")
  parser.add_argument("--result_folder", type=str, required=True, help="Folder to put results in")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  s3 = boto3.resource("s3")

  for bucket in ["maccoss-ec2", "maccoss-emr"]:
    s3.Bucket(bucket).objects.filter(Prefix="0/").delete()

  if not os.path.isdir(args.result_folder):
    os.mkdir(args.result_folder)
    if not os.path.isdir(args.result_folder + "/tasks"):
      os.mkdir(args.result_folder + "/tasks")
    if not os.path.isdir(args.result_folder + "/nodes"):
      os.mkdir(args.result_folder + "/nodes")
  shutil.copyfile(args.parameters, args.result_folder + "/" + args.parameters.split("/")[-1])
  m = master.Master(args.s3_application_url, args.result_folder, params)
  m.setup()
  m.start(asynch=True)
  simulation.run(params, m, False)
  m.shutdown()


if __name__ == "__main__":
  main()

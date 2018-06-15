import boto3
import os
import subprocess
import time

s3 = boto3.resource('s3')

def handler(event, context):
  print("here")
  bucket = s3.Bucket("human-fasta")
    
  with open("/tmp/HUMAN.fasta.20170123", "wb") as f:
    bucket.download_fileobj("HUMAN.fasta.20170123", f)

  with open("/tmp/crux", "wb") as f:
    bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  index_files = ["auxlocs", "pepix", "protix"]
  if not os.path.isdir("/tmp/HUMAN.fasta.20170123.index"):
    os.mkdir("/tmp/HUMAN.fasta.20170123.index")
  for index_file in index_files:
    with open("/tmp/HUMAN.fasta.20170123.index/{0:s}".format(index_file), "wb") as f:
      bucket.download_fileobj(index_file, f)
     
  print("yolo") 
  output = subprocess.check_output("/tmp/crux tide-search /tmp/small.ms2 /tmp/HUMAN.fasta.20170123.index", shell=True)
  print(output)
  print("done")

  print(subprocess.check_output(["ls", "/tmp"]))
  return "Hello World!"


#  output_file = "/tmp/small.txt"#.{0:s}-{1:s}.txt".format(event["start"], event["end"])
#  command = "./crux tide-search small.ms2 HUMAN.fasta.20170123.index"
#  command = "./comet.2018011.linux.exe -N/tmp/small sorted-small-{0:d}.ms2".format(event["start"])
#-F{0:s} -L{1:s} sorted-small.ms2".format(event["start"], event["end"]) 
#  print(command)
#  subprocess.call(command, shell=True)

#  done = False
#  while not done:
#    output = str(subprocess.check_output("ps aux | grep comet | grep small", shell=True))
#    done = len(output.split("\n")) == 1
#    time.sleep(1)
#
#  return os.path.isfile(output_file)
#  return True

import boto3
from botocore.client import Config
import json
import numpy
import os
import re
import subprocess
import sys
import threading
import time

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CWD, "lib"))

SPECTRA = re.compile("S\s\d+.*")
INTENSITY = re.compile("I\s+MS1Intensity\s+([0-9\.]+)")

class Task(threading.Thread):
  def __init__(self, thread_id, client, results):#scan, results):
    super(Task, self).__init__()
    self.client = client
    self.results = results
#    self.scan = scan
    self.thread_id = thread_id
 
  def run(self):
    start = time.time()
    arguments = { "start": self.thread_id }#self.scan[0], "end": self.scan[1] }
    payload = json.dumps(arguments)
    response = self.client.invoke(
      FunctionName='CometIndex',
      InvocationType='RequestResponse',
      LogType='Tail',
      Payload=payload,
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    end = time.time()

    found = response["Payload"].read()
    self.results[self.thread_id] = { "duration": end-start, "found": found }
    return


def function_exists(client, name):
  try:
    client.get_function(
      FunctionName=name
    )
    return True
  except client.exceptions.ResourceNotFoundException:
    return False 

def upload_function(client):
  name = "CometIndex"

  os.chdir("lambda")
  subprocess.call(["zip", "../lambda.zip", "*"])
  os.chdir("..")

  with open("lambda.zip", "rb") as f:
    zipped_code = f.read()

  if function_exists(client, name):
    response = client.update_function_code(
      FunctionName=name,
      ZipFile=zipped_code,
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

    response = client.update_function_configuration( 
      FunctionName=name,
      MemorySize=512,
      Timeout=5*60,
    )
  else:
    response = client.create_function(
      FunctionName=name,
      Runtime='python3.6',
      Role="arn:aws:iam::999145429263:role/service-role/lambdaFullAccessRole",
      Handler="main.handler",
      Code={ "ZipFile": zipped_code },
      Timeout=5*60,
      MemorySize=512,
    )

  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

def get_scans():
  output = subprocess.check_output("grep '^S' lambda/sorted-small.ms2 | awk '{print ""$2""}'", shell=True)
  output = output.split("\n")
  offset = 100
  scans = []
  for i in range(0, len(output), offset):
    scans.append((output[i], output[min(i+offset-1, len(output)-1)])) 
  return scans

def process():
  print("process")
  subprocess.call("rm lambda/sorted-small-*", shell=True)
  f = open("lambda/small.ms2")
  lines = f.readlines()[1:]
  f.close()

  spectrum = []
  intensity = None
  spectra = []

  for line in lines: 
    if SPECTRA.match(line):
      if intensity is not None:
        spectrum.append((intensity, "".join(spectra)))
        intensity = None
        spectra = []

    m = INTENSITY.match(line)
    if m: 
      intensity = float(m.group(1))

    spectra.append(line)

  spectrum = sorted(spectrum, key=lambda spectra: -spectra[0])

  offset = 260
  i = 0
  print("offset", offset)
  while i * offset < min(len(spectrum), 1):
    index = i * offset
    f = open("lambda/sorted-small-{0:d}.ms2".format(i), "w+")
    f.write("H Extractor MzXML2Search\n")
    for spectra in spectrum[index:min(index+offset, len(spectrum))]:
      for line in spectra[1]:
        f.write(line)
    i += 1
  return i

def run():
  config = Config(read_timeout=5*60)
  client = boto3.client("lambda", region_name="us-east-1", config=config)
#  num_threads = process()
  num_threads = 1
  upload_function(client)
#  scans = get_scans()[:100]
  print("Number of threads", num_threads)
  threads = []
  results = [None] * num_threads#len(scans)

  for i in range(num_threads):#len(scans)):
    thread = Task(i, client, results)#scans[i], results)
    thread.start()
    threads.append(thread) 

  joined = 0
  for i in range(len(threads)):
    threads[i].join()
    joined += 1
    if joined % 10 == 0:
      print(joined, "out of", len(threads), "joined")

  min_time = min(results)
  max_time = max(results)

  durations = map(lambda r: r["duration"], results)
  found = 0
  for result in results:
    if result["found"] == "true":
      found += 1
  payloads = map(lambda r: r["found"], results)
  print("Num found", found)
#  print(payloads)
  average = numpy.average(durations)
  var = numpy.var(durations)
  std = numpy.std(durations)
  print("min", min_time)
  print("max", max_time)
  print("avg", average)
  print("var", var)
  print("std", std)

run()

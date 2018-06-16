import argparse
import boto3
from botocore.client import Config
import json
import numpy
import os
import Queue
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
  def __init__(self, thread_id, client, requests, results, params):
    super(Task, self).__init__()
    self.client = client
    self.requests = requests
    self.results = results
    self.thread_id = thread_id
    self.params = params
 
  def run(self):
    while not self.requests.empty():
      try:
        request = self.requests.get()
      except Queue.Empty:
        continue

      arguments = { "start": request[0], "end": request[1] }
      payload = json.dumps(arguments)
      try:
        start = time.time()
        response = self.client.invoke(
          FunctionName=self.params["function_name"],
          InvocationType='RequestResponse',
          LogType='Tail',
          Payload=payload,
        )
        end = time.time()
        assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
        found = response["Payload"].read()
        self.results.put({ "duration": end-start, "found": found })
      except:
        end = time.time()
        self.results.put({ "duration": end-start, "found": False })

      if self.thread_id == 0:
        print("results", self.results.qsize())

def function_exists(client, name):
  try:
    client.get_function(
      FunctionName=name
    )
    return True
  except client.exceptions.ResourceNotFoundException:
    return False 

def upload_function(client, params):
  name = params["function_name"]

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
      MemorySize=params["memory_size"],
      Timeout=params["timeout"],
    )
  else:
    response = client.create_function(
      FunctionName=name,
      Runtime='python3.6',
      Role="arn:aws:iam::999145429263:role/service-role/lambdaFullAccessRole",
      Handler="main.handler",
      Code={ "ZipFile": zipped_code },
      Timeout=params["memory_size"],
      MemorySize=params["timeout"],
    )

  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

def get_requests(batch_size):
  requests = Queue.Queue()
  output = subprocess.check_output("grep '^S' lambda/small.ms2 | awk '{print ""$2""}'", shell=True)
  output = output.split("\n")
  for i in range(0, len(output), batch_size):
    start = output[i]
    end = output[min(i + batch_size - -1, len(output) - 1)]
    requests.put((start, end))
  return requests

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

def run(params):
  num_threads = params["num_threads"]
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client("lambda", region_name=params["region"], config=config)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

  upload_function(client, params)
  requests = get_requests(params["batch_size"])
  print("Number of requests", requests.qsize())
  results = Queue.Queue()
  threads = []

  for i in range(num_threads):
    thread = Task(i, client, requests, results, params)
    thread.start()
    threads.append(thread) 

  for i in range(len(threads)):
    threads[i].join()

  results = list(results.queue)
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

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing paramters") 
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params)
  
main()

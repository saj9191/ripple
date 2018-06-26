import argparse
import boto3
from botocore.client import Config
import json
import numpy
import os
import queue
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
      except queue.Empty:
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
        output = response["Payload"].read().strip()
        self.results.put({ "duration": end-start, "result": output })
      except Exception as e:
        end = time.time()
        self.results.put({ "duration": end-start, "result": str.encode("") })

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

def upload_functions(client, params):
  functions = ["split_spectra", "analyze_spectra", "combine_spectra_results"]
  names = ["SplitSpectra", "AnalyzeSpectra", "CombineSpectraResults"]

  os.chdir("lambda")
  for i in range(len(functions)):
    function = functions[i]
    name = names[i]
    subprocess.call("zip ../{0:s}.zip {0:s}.py".format(function), shell=True)

    with open("../{0:s}.zip".format(function), "rb") as f:
      zipped_code = f.read()

    response = client.update_function_code(
      FunctionName=name,
      ZipFile=zipped_code,
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)

  os.chdir("..")

def get_requests(batch_size):
  requests = queue.Queue()
  output = subprocess.check_output("grep '^S' lambda/small.ms2 | awk '{print ""$2""}'", shell=True).decode("utf-8")
  output = output.split("\n")
  for i in range(0, len(output), batch_size):
    start = output[i]
    end = output[min(i + batch_size -1, len(output) - 1)]
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

def combine_results(results):
  final_result = ""
  while not results.empty():
    result = results.get()
    output = result["result"].decode("utf-8")
    if len(final_result) == 0 and len(output) > 2:
      print("first")
      final_result += output
    elif len(output) > 2:
      print("second")
      t = output.split("\\n")
      print("t", t[0:2])
      s = list(filter(lambda o: not (o.startswith('H') or o.startswith('"H')), t))
      print("s", s[0:2])
      final_result += "\\n".join(s)
  return final_result

def run(params):
  num_threads = params["num_threads"]
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client("lambda", region_name=params["region"], config=config)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0

  upload_functions(client, params)
#  requests = get_requests(params["batch_size"])
  #print("Number of requests", requests.qsize())
#  results = queue.Queue()
#  threads = []

#  for i in range(num_threads):
#    thread = Task(i, client, requests, results, params)
#    thread.start()
#    threads.append(thread) 

#  for i in range(len(threads)):
#    threads[i].join()

#  final_result = combine_results(results)
#  f = open("temp", "wb")
#  for line in final_result.split("\\n"):
#    if line.startswith('"'):
#      line = line[1:]
#    f.write(str.encode("\t".join(line.split("\\t")) + "\n"))
#  f.close()
#  min_time = min(results)
#  max_time = max(results)

#  durations = map(lambda r: r["duration"], results)
#  found = 0
#  for result in results:
#    if result["found"] == "true":
#      found += 1
#  payloads = map(lambda r: r["found"], results)
#  print("Num found", found)
#  print(payloads)
#  average = numpy.average(durations)
#  var = numpy.var(durations)
#  std = numpy.std(durations)
#  print("min", min_time)
#  print("max", max_time)
#  print("avg", average)
#  print("var", var)
#  print("std", std)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing paramters") 
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(params)
  
main()

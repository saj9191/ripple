import boto3
import benchmark
import inspect
import os
import sys
import threading
import time
import util
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import confidence

s3 = boto3.resource("s3")
bucket = s3.Bucket("shjoyner-logs")

token_to_scores = {}
token_to_file = {}


class Request(threading.Thread):
  def __init__(self, thread_id, time, file_name, params):
    super(Request, self).__init__()
    self.time = time
    self.file_name = file_name
    self.params = dict(params)
    self.thread_id = thread_id

  def run(self):
    [access_key, secret_key] = util.get_credentials("default")
    self.params["input_name"] = self.file_name
    self.params["access_key"] = access_key
    self.params["secret_key"] = secret_key
    print("Thread {0:d}: Processing file {1:s}".format(self.thread_id, self.file_name))
    [upload_duration, duration, failed_attempts] = benchmark.run(self.params, self.thread_id)

    token = "{0:f}-{1:d}".format(self.params["now"], self.params["nonce"])
    token_to_file[token] = self.file_name


def run(bucket_name, prefix, token=None):
  bucket = s3.Bucket(bucket_name)
  keys = []
  if token is not None:
    prefix += token + "/"

  num_keys = None
  while num_keys is None or len(keys) < num_keys:
    try:
      keys = list(map(lambda o: o.key, list(bucket.objects.filter(Prefix=prefix))))
      if len(keys) > 0:
        num_keys = util.parse_file_name(keys[0])["num_files"]
    except Exception as e:
      print("Error reading", e)
      keys = []
    time.sleep(10)
  keys.sort(key=lambda k: util.parse_file_name(k)["suffix"])

  species_to_score = {}

  print("Processing...")
  for i in range(len(keys)):
    obj = s3.Object(bucket_name, keys[i])
    it = confidence.Iterator(obj, 10*1000)
    s = it.sum("q-value")
    specie = util.parse_file_name(obj.key)["suffix"]
    species_to_score[specie] = s
    if s > 0:
      print(keys[i])
      print("***", i+2, specie, s)
    # else:
    #   print(i+2, util.parse_file_name(obj.key)["suffix"], s)
  return species_to_score

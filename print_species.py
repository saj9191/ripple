import boto3
import benchmark
import json
import threading
import util

s3 = boto3.resource("s3")
bucket = s3.Bucket("shjoyner-logs")
prefix = "5/"

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


params = json.loads(open("json/tide.json").read())
[access_key, secret_key] = util.get_credentials("default")
params["access_key"] = access_key
params["secret_key"] = secret_key
params["setup"] = False
params["stats"] = False
params["iterations"] = 1
params["sample_input"] = True
params["params_name"] = "json/tide.json"

requests = []
threads = []
print("sample", params["sample_bucket"])
file_names = list(map(lambda o: o.key, s3.Bucket(params["sample_bucket"]).objects.all()))[20:]

for i in range(len(file_names)):
  thread = Request(i, 0, file_names[i % len(file_names)], params)
  thread.start()
  threads.append(thread)

for thread in threads:
  thread.join()


objects = list(bucket.objects.filter(Prefix=prefix))
for i in range(len(objects)):
  if i % 100 == 0:
    print("Through", float(i) / len(objects) * 100, "%")
  obj = objects[i]
  token = obj.key.split("/")[-3]
  if token not in token_to_scores:
    token_to_scores[token] = []

  content = obj.get()["Body"].read().decode("utf-8")
  lines = content.split("\n")
  for line in lines:
    if "score" in line:
      parts = line.split(" ")
      score = int(parts[3])
      key = parts[1].split("/")[-1].split("-")[-1].split(".")[0]
      token_to_scores[token].append([key, score])

keys = sorted(token_to_scores.keys(), key=lambda k: token_to_file[k])
for token in keys:
  print("File", token_to_file[token])
  if len(token_to_scores[token]) < 60:
    print("Cannot find", token)
    continue
  for p in token_to_scores[token]:
    if p[1] > 0:
      print("\tkey", p[0], p[1])
  print("")

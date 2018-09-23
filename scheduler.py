import argparse
import json
import queue
import threading
import time
import util


class Worker(threading.Thread):
  def __init__(self, worker_id, pending, timeout, params, event, client):
    super(Worker, self).__init__()
    self.worker_id = worker_id
    self.pending = pending
    self.running = True
    self.timeout = timeout
    self.params = dict(params)
    self.event = event
    self.client = client

  def run(self):
    functions = self.params["functions"]
    pipeline = self.params["pipeline"]
    s3 = util.s3(self.params)

    while self.running:
      try:
        [prefix, payload] = self.pending.get(timeout=self.timeout)
        if prefix < len(pipeline):
          name = pipeline[prefix]["name"]
          util.invoke(self.client, name, self.params, payload)
          s = payload["Records"][0]["s3"]
          input_key = s["object"]["key"]
          rparams = {**self.params, **s}
          rparams["prefix"] = prefix
          rparams["scheduler"] = True
          [input_format, output_format, bucket_format] = util.get_formats(input_key, functions[name]["file"], rparams)
          log_file = util.file_name(bucket_format)
          while not util.object_exists(self.params["log"], log_file):
            time.sleep(5)
          content = s3.Object(self.params["log"], log_file).get()["Body"].read()
          lparams = json.loads(content.decode("utf-8"))
          for p in lparams["payloads"]:
            self.pending.put([prefix + 1, p])
        else:
          print("Worker", self.worker_id, "Found final")
          self.event.set()
      except queue.Empty:
        time.sleep(5)


def schedule(start_key, params):
  max_running_lambdas = 1000
  pending = queue.Queue()
  workers = []
  params["object"] = {}
  params["scheduler"] = False
  timeout = 10
  event = threading.Event()
  client = util.setup_client("lambda", params)
  for i in range(max_running_lambdas):
    workers.append(Worker(i, pending, timeout, params, event, client))
    workers[-1].start()

  pending.put(
    [
      0,
      {
        "Records": [{
          "s3": {
            "bucket": {
              "name": params["bucket"],
            },
            "object": {
              "key": start_key,
            },
          }
        }]
      }
    ]
  )
  event.wait()
  for worker in workers:
    worker.running = False

  for worker in workers:
    worker.join()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())

  [access_key, secret_key] = util.get_credentials(params["credential_profile"])
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  schedule(params)


if __name__ == "__main__":
  main()

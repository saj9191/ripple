import argparse
import json
import time
import util


def schedule(params):
  # TODO: For now, just make a list
  pipeline = params["pipeline"]
  functions = params["functions"]
  start_key = "0/1537670657.440274-565/1/1-1-tide.mzML"
  running = []
  pending = [
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
  ]
  params["object"] = {}
  max_running_lambdas = 1000
  client = util.setup_client("lambda", params)
  s3 = util.s3(params)
  stages = set()

  while (len(running) + len(pending)) > 0:
    i = 0
    print("Running", len(running), "Pending", len(pending))
    while i < len(running):
      log_file = running[i]
      m = util.parse_file_name(log_file)
      prefix = m["prefix"] - 1
      name = pipeline[prefix]["name"]
      params["prefix"] = prefix
      # TODO: Need to know what stage we're on
      params["scheduler"] = True
      if util.object_exists(params["log"], log_file):
        content = s3.Object(params["log"], log_file).get()["Body"].read()
        lparams = json.loads(content.decode("utf-8"))
        pending += list(map(lambda p: [prefix + 1, p], lparams["payloads"]))
        del running[i]
      else:
        i += 1

    while len(running) < max_running_lambdas and len(pending) > 0:
      [prefix, payload] = pending.pop(0)
      if prefix < len(pipeline):
        params["scheduler"] = False
        name = pipeline[prefix]["name"]
        util.invoke(client, name, params, payload)
        s = payload["Records"][0]["s3"]
        input_key = s["object"]["key"]
        rparams = {**params, **s}
        rparams["prefix"] = prefix
        rparams["scheduler"] = True
        [input_format, output_format, bucket_format] = util.get_formats(input_key, functions[name]["file"], rparams)
        log_file = util.file_name(bucket_format)
        running.append(log_file)
        if prefix not in stages:
          print("Starting stage", prefix, name)
          stages.add(prefix)

    time.sleep(5)


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

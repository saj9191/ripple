import argparse
import benchmark
import boto3
import json
import plot


class Results:
  def __init__(self, params):
    self.params = params


def clear():
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("shjoyner-tide")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-logs")
  bucket.objects.all().delete()
  bucket = s3.Bucket("shjoyner-ssw")
  bucket.objects.all().delete()


def trigger_plot(folder):
  folder_timestamp = float(folder.split("/")[-1])
  params = json.loads(open("{0:s}/params.json".format(folder)).read())
  params["timestamp"] = folder_timestamp

  f = open("{0:s}/files".format(folder))
  lines = f.readlines()
  results = []
  for line in lines:
    line = line.strip()
    parts = line.split("-")
    timestamp = float(parts[0])
    nonce = int(parts[1])
    p = dict(params)
    p["now"] = timestamp
    p["nonce"] = nonce
    results.append(Results(p))
    stats = json.loads(open("{0:s}/{1:s}/stats".format(folder, line)).read())
    deps = benchmark.create_dependency_chain(stats["stats"], 1)
    with open("{0:s}/{1:s}/deps".format(folder, line), "w+") as f:
      json.dump(deps, f, default=benchmark.serialize, sort_keys=True, indent=4)

  plot.plot(results, params["pipeline"], params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--clear', action="store_true", help="Clear bucket files")
  parser.add_argument('--plot', type=str, help="Plot graph")
  args = parser.parse_args()

  if args.clear:
    clear()
  if args.plot:
    trigger_plot(args.plot)


if __name__ == "__main__":
  main()

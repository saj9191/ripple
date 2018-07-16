import boto3
import json
import os
import util


def combine_files(s3, bucket_name, keys, temp_file):
  f = open(temp_file, "w")
  keys.sort()

  for i in range(len(keys)):
    key = keys[i]
    spectra = s3.Object(bucket_name, key).get()["Body"].read().decode("utf-8")

    if i == 0:
      f.write(spectra)
    else:
      results = spectra.split("\n")[1:]
      f.write("\n".join(results))


def combine(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  num_bytes = m["max_id"]

  _, ext = os.path.splitext(key)
  key_regex = util.get_key_regex(ts, num_bytes, "txt")

  [have_all_files, matching_keys] = util.have_all_files(bucket_name, num_bytes, key_regex)

  if have_all_files:
    print(ts, "Combining", len(matching_keys))
    temp_file = "/tmp/combine.txt"
    s3 = boto3.resource("s3")
    combine_files(s3, bucket_name, matching_keys, temp_file)
    file_name = util.file_name(ts, 1, 1, 1, "txt")
    s3.Object(params["output_bucket"], file_name).put(Body=open(temp_file, 'rb'))
  else:
    print(ts, "Passing", len(matching_keys))
    pass


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("combine_spectra_results.json").read())
  combine(bucket_name, key, params)

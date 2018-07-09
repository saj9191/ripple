import boto3
import re
import util

RESULT_FILE = util.spectra_regex("txt")


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


def combine(bucket_name, output_file):
  util.clear_tmp()
  m = RESULT_FILE.match(output_file)
  ts = m.group(1)
  num_bytes = int(m.group(4))

  s3 = boto3.resource("s3")

  file_format = "spectra-{0:s}-([0-9]+)-([0-9]+)-{1:d}.txt".format(ts, num_bytes)
  key_regex = re.compile(file_format)

  [have_all_files, matching_keys] = util.have_all_files(bucket_name, num_bytes, key_regex)

  if have_all_files:
    print(ts, "Combining", len(matching_keys))
    temp_file = "/tmp/combine.txt"
    combine_files(s3, bucket_name, matching_keys, temp_file)
    s3.Object(bucket_name, "tide-search-{0:s}.txt".format(ts)).put(Body=open(temp_file, 'rb'))
  else:
    print(ts, "Passing", len(matching_keys))
    pass


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  output_file = event["Records"][0]["s3"]["object"]["key"]
  combine(bucket_name, output_file)

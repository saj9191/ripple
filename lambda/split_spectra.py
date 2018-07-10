import boto3
import json
import re
import util


def save_spectra(output_bucket, spectra, ts, file_id, byte_id, num_bytes):
  key = util.file_name(ts, file_id, byte_id, num_bytes, "ms2")
  output_bucket.put_object(Key=key, Body=str.encode(spectra))


def split_spectra(bucket_name, key, params):
  util.clear_tmp()
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket(params["output_bucket"])

  obj = s3.Object(bucket_name, key)
  num_bytes = obj.content_length
  spectra = []
  remainder = ""
  header = None

  m = util.parse_file_name(key)
  ts = m["timestamp"]

  start_byte = 0
  file_id = 1

  while start_byte < num_bytes:
    end_byte = min(start_byte + chunk_size, num_bytes)
    stream = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")
    stream = remainder + stream

    if start_byte == 0:
      parts = stream.split("\n")
      # Remove header
      stream = "\n".join(parts[1:])

    [new_spectra, remainder] = util.parse_spectra(stream)

    if len(new_spectra) > 1:
      spectra += new_spectra
      while len(spectra) >= batch_size:
        if end_byte == num_bytes and len(spectra) <= batch_size and len(remainder.strip()) == 0:
          start_byte = num_bytes
        else:
          if start_byte >= num_bytes:
            print("ERROR", start_byte, num_bytes)
          assert(start_byte < num_bytes)
        save_spectra(output_bucket, "".join(spectra[:batch_size]), ts, file_id, start_byte, num_bytes)
        spectra = spectra[batch_size:]
        file_id += 1

    start_byte = end_byte + 1

  if len(remainder.strip()) > 0:
    spectra = spectra + [remainder]
  assert(len(spectra) > 0)
  save_spectra(output_bucket, "".join(spectra), ts, file_id, num_bytes, num_bytes)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params)

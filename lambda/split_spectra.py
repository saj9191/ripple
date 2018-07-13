import boto3
import json
import util


def save_spectra(output_bucket, spectra, ts, file_id, byte_id, num_bytes):
  s = list(map(lambda spectrum: spectrum.group(0), spectra))
  key = util.file_name(ts, file_id, byte_id, num_bytes, "ms2")
  output_bucket.put_object(Key=key, Body=str.encode("".join(s)))


def split_spectra(bucket_name, key, params):
  util.clear_tmp()
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket(params["output_bucket"])

  obj = s3.Object(bucket_name, key)
  num_bytes = obj.content_length
  spectra_regex = []
  remainder = ""

  m = util.parse_file_name(key)
  ts = m["timestamp"]

  start_byte = 0
  file_id = 1

  while start_byte < num_bytes:
    end_byte = min(start_byte + chunk_size, num_bytes)
    [new_spectra_regex, remainder] = util.get_spectra(obj, start_byte, end_byte, num_bytes, remainder)
    spectra_regex += new_spectra_regex

    while len(spectra_regex) >= batch_size:
      batch = spectra_regex[:batch_size]
      byte_id = batch[0].span(0)[0]

      save_spectra(output_bucket, batch, ts, file_id, byte_id, num_bytes)
      spectra_regex = spectra_regex[batch_size:]
      file_id += 1

    start_byte = end_byte + 1

  assert(len(remainder.strip()) == 0)
  save_spectra(output_bucket, spectra_regex, ts, file_id, num_bytes, num_bytes)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params)

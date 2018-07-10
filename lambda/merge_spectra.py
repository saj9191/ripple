import boto3
import json
import re
import time
import util

class Spectra:
  def __init__(self, obj, start_byte, num_bytes, spectra, remainder):
    self.obj = obj
    self.start_byte = start_byte
    self.num_bytes = num_bytes
    self.spectra = spectra
    self.remainder = remainder


def extractMass(spectrum):
  lines = spectrum.split("\n")
  m = list(filter(lambda line: util.MASS.match(line), lines))
  # assert(len(m) == 1) # TODO: Handle multiple later
  first = m[0]
  mass = util.MASS.match(m[0]).group(2)
  return (float(mass), spectrum)


def get_spectra(obj, start_byte, end_byte, num_bytes, remainder):
  if start_byte >= num_bytes:
    return ([], "")

  end_byte = min(num_bytes, end_byte)
  stream = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")
  if len(remainder.strip()) > 0:
    stream = remainder + stream

  parts = util.SPECTRA_START.split(stream)

  # If the stream is "S\t123....", then parts will become ["", "123..."]
  if len(parts[0]) == 0:
    parts = parts[1:]
  parts = list(map(lambda p: "S\t" + p, parts))
  s = "".join(parts)

  if end_byte < num_bytes:
    remainder = parts[-1]
    parts = parts[:-1]
  else:
    remainder = ""

  spectra = list(map(lambda p: extractMass(p), parts))
  spectra.sort(key=util.getMass)

  return (spectra, remainder)


def save_spectra(output_bucket, spectra, ts, file_id, num_files):
  s = "".join(spectra)
  key = util.file_name(ts, file_id, file_id, num_files, "ms2")
  output_bucket.put_object(Key=key, Body=str.encode(s))


def createFileObjects(s3, bucket_name, matching_keys, chunk_size):
  files = []
  for matching_key in matching_keys:
    obj = s3.Object(bucket_name, matching_key)
    num_bytes = obj.content_length

    start_byte = 0
    end_byte = chunk_size
    [spectra, remainder] = get_spectra(obj, start_byte, end_byte, num_bytes, "")

    files.append(Spectra(obj, end_byte, num_bytes, spectra, remainder))

  files.sort(key=lambda p: util.getMass(p.spectra[0]))
  return files


def merge_spectra(bucket_name, key, params):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket(params["output_bucket"])
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  m = util.parse_file_name(key)
  ts = m["timestamp"]
  num_bytes = m["max_id"]

  print("MERGE", key)
  if m["id"] != num_bytes:
    print(ts, "Passing")
    return

  key_regex = util.get_key_regex(ts, num_bytes)
  have_all_files = False
  matching_keys = []
  while not have_all_files:
    [have_all_files, matching_keys] = util.have_all_files(bucket_name, num_bytes, key_regex)
    time.sleep(1)

  print("Combining", len(matching_keys), "files", key)
  num_files = len(matching_keys)
  files = createFileObjects(s3, bucket_name, matching_keys, chunk_size)
  spectra = []
  file_id = 1
  count = 0
  while len(files) > 0:
    f = files[0]
    next_spectrum = f.spectra[0]

    spectra.append(next_spectrum[1])
    count += 1
    if len(spectra) == batch_size:
      if file_id > num_files:
        print("ERROR", file_id, num_files)
      save_spectra(output_bucket, spectra, ts, file_id, num_files)
      file_id += 1
      spectra = []

    f.spectra = f.spectra[1:]
    files = files[1:]

    if len(f.spectra) == 0:
      start_byte = f.start_byte
      end_byte = start_byte + chunk_size
      [fspectra, remainder] = get_spectra(f.obj, start_byte, end_byte, f.num_bytes, f.remainder)
      f.spectra = fspectra
      f.remainder = remainder
      f.start_byte = end_byte + 1

    if len(f.spectra) > 0:
      index = 0
      while index < len(files) and util.getMass(files[index].spectra[0]) < util.getMass(f.spectra[0]):
        index += 1

      before_files = files[:index]
      after_files = []
      if index != len(files):
        after_files = files[index:]
      files = before_files + [f] + after_files

  print("HELLO COUNT", count)
  if len(spectra) > 0:
    assert(file_id == len(matching_keys))
    save_spectra(output_bucket, spectra, ts, file_id, num_files)
  else:
    assert(file_id > len(matching_keys))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("merge_spectra.json").read())
  merge_spectra(bucket_name, key, params)

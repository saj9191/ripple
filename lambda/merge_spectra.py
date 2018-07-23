import boto3
import heapq
import json
import time
import util


class Spectra:
  def __init__(self, obj, start_byte, num_bytes, spectra, remainder):
    self.obj = obj
    self.start_byte = start_byte
    self.num_bytes = num_bytes
    self.spectra = spectra
    self.remainder = remainder

  # If the masses of the spectra are equal, heapq tries to use the second element, which is a spectra to compare.
  def __lt__(self, other):
    return True


def save_spectra(output_bucket, spectra, ts, file_id, num_files):
  s = "\n".join(spectra)
  key = util.file_name(ts, file_id, file_id, num_files, "ms2")
  output_bucket.put_object(Key=key, Body=str.encode(s))


def get_spectra(obj, start_byte, end_byte, num_bytes, remainder):
  [spectra_regex, remainder] = util.get_spectra(obj, start_byte, end_byte, num_bytes, remainder)
  spectra_regex = list(map(lambda spectrum: (float(spectrum.group(1)), spectrum.group(0)), spectra_regex))
  return (spectra_regex, remainder)


def createFileObjects(s3, bucket_name, matching_keys, chunk_size):
  files = []
  for matching_key in matching_keys:
    obj = s3.Object(bucket_name, matching_key)
    num_bytes = obj.content_length

    start_byte = 0
    end_byte = 0
    remainder = ""
    spectra_regex = []
    while len(spectra_regex) == 0:
      end_byte = start_byte + chunk_size
      [new_spectra_regex, remainder] = get_spectra(obj, start_byte, end_byte, num_bytes, remainder)
      spectra_regex += new_spectra_regex
      start_byte = end_byte + 1

    s = Spectra(obj, end_byte, num_bytes, spectra_regex, remainder)
    heapq.heappush(files, (spectra_regex[0][0], s))

  return files


def merge_spectra(bucket_name, key, params):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  print("TIMESTAMP {0:f}".format(ts))

  output_bucket = s3.Bucket(params["output_bucket"])
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]

  max_bytes = m["max_id"]

  if m["id"] != max_bytes:
    print(ts, "Passing")
    return

  key_regex = util.get_key_regex(ts, max_bytes)
  have_all_files = False
  matching_keys = []
  while not have_all_files:
    [have_all_files, matching_keys] = util.have_all_files(bucket_name, max_bytes, key_regex)
    time.sleep(1)

  print("Combining", len(matching_keys), "files", key)
  num_files = len(matching_keys)
  files = createFileObjects(s3, bucket_name, matching_keys, chunk_size)
  spectra = []
  file_id = 1

  while len(files) > 0:
    [_, f] = heapq.heappop(files)
    next_spectrum = f.spectra.pop()
    spectra.append(next_spectrum[1])

    if len(spectra) == batch_size:
      save_spectra(output_bucket, spectra, ts, file_id, num_files)
      file_id += 1
      spectra = []

    while len(f.spectra) == 0 and f.start_byte <= f.num_bytes:
      start_byte = f.start_byte
      end_byte = start_byte + chunk_size
      [new_spectra_regex, remainder] = get_spectra(f.obj, start_byte, end_byte, f.num_bytes, f.remainder)
      f.spectra = new_spectra_regex
      f.remainder = remainder
      f.start_byte = end_byte + 1

    if len(f.spectra) > 0:
      heapq.heappush(files, (f.spectra[0][0], f))

  while len(spectra) > 0:
    length = min(batch_size, len(spectra))
    s = spectra[:length]

    save_spectra(output_bucket, s, ts, file_id, num_files)
    spectra = spectra[length:]
    if len(spectra) > 0:
      file_id += 1

  assert(file_id == len(matching_keys))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("merge_spectra.json").read())
  merge_spectra(bucket_name, key, params)

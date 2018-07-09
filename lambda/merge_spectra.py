import boto3
import json
import re
import util


INPUT_FILE = util.spectra_regex("ms2", "sorted-")


class Spectra:
  def __init__(self, obj, start_byte, num_bytes, spectra, remainder):
    self.obj = obj
    self.start_byte = start_byte
    self.num_bytes = num_bytes
    self.spectra = spectra
    self.remainder = remainder


def extractMass(spectrum):
  m = util.MASS.match(spectrum)
  assert(m is not None)
  return (int(m.group(2)), spectrum)


def get_spectra(obj, start_byte, end_byte, num_bytes, remainder):
  if start_byte >= num_bytes:
    return ([], "")

  end_byte = min(num_bytes, end_byte)
  stream = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")
  stream = remainder + stream
  parts = util.SPECTRA_START.split(stream)
  parts = list(filter(lambda p: len(p) > 0, parts))

  spectra = []
  remainder = ""

  spectra = list(map(lambda p: extractMass(p), parts[:-1]))
  spectra.sort(key=util.getMass)
  remainder = parts[-1]
  return (spectra, remainder)


def save_spectra(output_bucket, spectra, ts, num_files, i):
  key = "spectra-{0:f}-{1:d}-{2:d}.ms2".format(ts, num_files, i)
  output_bucket.put_object(Key=key, Body=str.encode("".join(spectra)))


def createFileObjects(s3, bucket_name, matching_keys, chunk_size):
  files = []
  for matching_key in matching_keys:
    obj = s3.Object(bucket_name, matching_key)
    num_bytes = obj.content_length

    start_byte = 0
    end_byte = chunk_size
    [spectra, remainder] = get_spectra(obj, start_byte, end_byte, "")

    files.append(Spectra(obj, chunk_size, num_bytes, spectra, remainder))

  files.sort(key=lambda p: util.getMass(p.spectra[0]))
  return files


def merge_spectra(bucket_name, key, batch_size, chunk_size):
  util.clear_tmp()
  s3 = boto3.resource("s3")
  output_bucket = s3.Bucket("maccoss-human-split-spectra")

  m = INPUT_FILE.match(key.key)
  ts = m.group(1)
  num_bytes = int(m.group(4))

  file_format = "sorted-spectra-{0:s}-([0-9]+)-([0-9]+)-{1:d}.ms2".format(ts, num_bytes)
  key_regex = re.compile(file_format)

  [have_all_files, matching_keys] = util.have_all_files(bucket_name, num_bytes, key_regex)

  if not have_all_files:
    print(ts, "Passing", len(matching_keys))
    return

  files = createFileObjects(s3, bucket_name, matching_keys, chunk_size)
  spectra = []
  i = 1
  while len(files) > 0:
    f = files[0]
    next_spectrum = f.spectra[0]
    spectra.append(next_spectrum[1])
    if len(spectra) == batch_size:
      save_spectra(spectra, ts, i, len(matching_keys))
      i += 1
      spectra = []

    f.spectra = f.spectra[1:]
    files = files[1:]

    if len(f.spectra) == 0:
      start_byte = f.start_byte
      end_byte = start_byte + chunk_size
      [fspectra, remainder] = get_spectra(f.obj, start_byte, end_byte, f.num_bytes, f.remainder)
      f.spectra = fspectra
      f.remainder = remainder

    if len(f.spectra) > 0:
      index = 0
      while index < len(files) and util.getMass(files[index].spectra[0]) < util.getMass(f.spectra[0]):
        index += 1

      before_files = files[:index]
      if index == len(files):
        after_files = []
      else:
        after_files = files[index:]

      files = before_files + [f] + after_files
    else:
      f.obj.close()

  if len(spectra) > 0:
    assert(i == len(matching_keys))
    save_spectra(output_bucket, spectra, ts, i, i)
  else:
    assert(i > len(matching_keys))


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("merge_spectra.json").read())
  merge_spectra(bucket_name, key, params["batch_size"], params["chunk_size"])

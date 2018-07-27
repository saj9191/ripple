import boto3
from botocore.client import Config
import os
import re
import subprocess

#  spectra-<timestamp>-<nonce>-<file-id>-<split>-<id>-<max-id>.<ext>
FILE_FORMAT = "{0:s}-{1:f}-{2:d}-{3:d}-{4:d}-{5:d}-{6:d}.{7:s}"
FILE_REGEX = re.compile(".*-([0-9\.]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)\.([A-Za-z]+)")
SPECTRA = re.compile("^\S[A-Ya-y0-9\s\.\+]+Z\s[0-9]+\s([0-9\.e\+]+)\n+([0-9\.\se\+]+)", re.MULTILINE)

def setup_client(service, params):
  extra_time = 20
  config = Config(read_timeout=params["timeout"] + extra_time)
  client = boto3.client(service,
                        aws_access_key_id=params["access_key"],
                        aws_secret_access_key=params["secret_key"],
                        region_name=params["region"],
                        config=config
                        )
  return client


def create_client(params):
  client = setup_client("lambda", params)
  # https://github.com/boto/boto3/issues/1104#issuecomment-305136266
  # boto3 by default retries even if max timeout is set. This is a workaround.
  client.meta.events._unique_id_handlers['retry-config-lambda']['handler']._checker.__dict__['_max_attempts'] = 0
  return client


def get_credentials(name):
  home = os.path.expanduser("~")
  f = open("{0:s}/.aws/credentials".format(home))
  lines = f.readlines()
  for i in range(len(lines)):
    header = "[{0:s}]".format(name)
    if lines[i].strip() == header:
      access_key = lines[i + 1].split("=")[1].strip()
      secret_key = lines[i + 2].split("=")[1].strip()
      return [access_key, secret_key]


def file_name(timestamp, nonce, file_id, id, max_id, ext, split=0, prefix="spectra"):
  return FILE_FORMAT.format(prefix, timestamp, nonce, file_id, split, id, max_id, ext)


def parse_file_name(file_name):
  m = FILE_REGEX.match(file_name)
  timestamp = float(m.group(1))
  nonce = int(m.group(2))
  file_id = int(m.group(3))
  split = int(m.group(4))
  id = int(m.group(5))
  max_id = int(m.group(6))
  ext = m.group(7)
  return {
    "timestamp": timestamp,
    "nonce": nonce,
    "file_id": file_id,
    "split": split,
    "id": id,
    "max_id": max_id,
    "ext": ext
  }


def get_key_regex(ts, num_bytes, ext="ms2", prefix=""):
  regex = FILE_FORMAT
  for i in range(2, 6):
    regex = regex.replace("{" + str(i) + ":d}", "([0-9]+)")
  regex = regex.replace("{6:d}", "{2:d}").replace("{7:s}", ext)
  if len(prefix) == 0:
    prefix = ".*"
  return re.compile(regex.format(prefix, ts, num_bytes))


def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)


def have_all_files(bucket_name, num_bytes, key_regex):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)

  matching_keys = []
  num_files = None
  splits = set()
  for key in bucket.objects.all():
    if key_regex.match(key.key):
      m = parse_file_name(key.key)
      matching_keys.append(key.key)
      if m["split"] != 0:
        splits.add(m["file_id"])
      if m["id"] == m["max_id"]:
        num_files = m["file_id"]

  if num_files is not None:
    num_files += len(splits)
  return (len(matching_keys) == num_files, matching_keys)


def getMass(spectrum):
  return spectrum[0]


def get_spectra(obj, start_byte, end_byte, num_bytes, remainder):
  if len(remainder.strip()) == 0 and start_byte >= num_bytes:
    return ([], "")

  if start_byte < num_bytes:
    end_byte = min(num_bytes, end_byte)
    stream = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")
  else:
    stream = ""

  stream = remainder + stream
  spectra_regex = list(constants.SPECTRA.finditer(stream))
  if len(spectra_regex) > 0:
    spectra_end_byte = spectra_regex[-1].span(0)[1]
    remainder = stream[spectra_end_byte:]
  else:
    remainder = stream.strip()

  return (spectra_regex, remainder)

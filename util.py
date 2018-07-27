import boto3
from botocore.client import Config
import os
import re
import subprocess


FILE_FORMAT = [{
  "name": "prefix",
  "type": "any",
}, {
  "name": "timestamp",
  "type": "float",
}, {
  "name": "nonce",
  "type": "int",
}, {
  "name": "file-id",
  "type": "int",
}, {
  "name": "last",
  "type": "bool",
}]


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


def file_format(m):
  name = ""
  for part in FILE_FORMAT:
    if len(name) > 0:
      name += "-"
    if part["name"] in m:
      value = m[part["name"]]
      if part["type"] == "any":
        name += value
      elif part["type"] == "bool":
        name += str(int(value))
      else:
        name += str(value)
    else:
      if part["type"] == "any":
        name += "(.*)"
      elif part["type"] == "float":
        name += "([0-9\.]+)"
      elif part["type"] == "int":
        name += "([0-9]+)"
      else:
        name += "([0-1])"
  name += "."
  if "ext" in m:
    name += m["ext"]
  else:
    name += "([A-Za-z0-9]+)"

  return name


def file_name(m):
  return file_format(m)


def parse_file_name(file_name):
  regex = re.compile(file_format({}))
  m = regex.match(file_name)
  p = {}
  i = 0
  for i in range(len(FILE_FORMAT)):
    part = FILE_FORMAT[i]
    name = part["name"]
    value = m.group(i+1)
    if part["type"] == "int":
      p[name] = int(value)
    elif part["type"] == "float":
      p[name] = float(value)
    elif part["type"] == "bool":
      p[name] = value == "1"
    else:
      p[name] = value

  p["ext"] = m.group(len(FILE_FORMAT) + 1)
  return p


def get_key_regex(m):
  return re.compile(file_format(m))


def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)


def have_all_files(bucket_name, key_regex):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)

  matching_keys = []
  num_files = None
  splits = set()
  for key in bucket.objects.all():
    if key_regex.match(key.key):
      m = parse_file_name(key.key)
      matching_keys.append(key.key)
      if m["last"]:
        num_files = m["file-id"]

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

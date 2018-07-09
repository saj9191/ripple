import boto3
import re
import subprocess

MASS = re.compile("Z\s+([0-9\.]+)\s+([0-9\.]+)")
SPECTRA = re.compile("S\s+([0-9\.]+)\s+([0-9\.]+)\s+([0-9\.]+)*")
SPECTRA_START = re.compile("S\s")


def spectra_regex(suffix, prefix=""):
  return re.compile("{0:s}spectra-([0-9\.]+)-([0-9]+)-([0-9]+)-([0-9]+).{1:s}".format(prefix, suffix))


def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)


def get_next_spectra(lines, start_index):
  index = start_index
  mass = None
  spectrum = []

  for line in lines[start_index:]:
    m = SPECTRA.match(line)
    if m:
      if mass is not None:
        return (mass, "".join(spectrum), index)

    m = MASS.match(line)
    if m:
      mass = float(m.group(2))
    spectrum.append(line)
    index += 1

  # Add the last element
  if len(spectrum) > 0:
    assert(mass is not None)
    return (mass, "".join(spectrum), -1)

  return (-1, "", -1)


def have_all_files(bucket_name, num_bytes, key_regex):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)

  matching_keys = []
  num_files = None
  for key in bucket.objects.all():
    m = key_regex.match(key.key)
    if m:
      matching_keys.append(key.key)
      if int(m.group(2)) == num_bytes:
        num_files = int(m.group(1)) + 1

  return (len(matching_keys) == num_files, matching_keys)


def getMass(spectrum):
  return spectrum[0]

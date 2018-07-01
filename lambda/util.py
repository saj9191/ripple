import re
import subprocess

def spectra_regex(suffix):
    return re.compile("spectra-([0-9\.]+)-([0-9]+)-([0-9]+)-([0-9]+).{0:s}".format(suffix))

def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)

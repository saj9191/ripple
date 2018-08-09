import re


MASS = re.compile(".*Z\s+([0-9\.]+)\s+([0-9\.]+).*")
SPECTRA_START = re.compile("S\s")
#  spectra-<timestamp>-<nonce>-<file-id>-<split>-<id>-<max-id>.<ext>

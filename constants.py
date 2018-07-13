import re


MASS = re.compile(".*Z\s+([0-9\.]+)\s+([0-9\.]+).*")
SPECTRA = re.compile("^\S[A-Ya-y0-9\s\.\+]+Z\s[0-9]+\s([0-9\.e\+]+)\n+([0-9\.\se\+]+)", re.MULTILINE)
SPECTRA_START = re.compile("S\s")
#  spectra-<timestamp>-<file-id>-<id>-<max-id>.<ext>
FILE_FORMAT = "spectra-{0:f}-{1:d}-{2:d}-{3:d}.{4:s}"
FILE_REGEX = re.compile("spectra-([0-9\.]+)-([0-9]+)-([0-9]+)-([0-9]+)\.([a-z]+)")

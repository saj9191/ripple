import re


MASS = re.compile(".*Z\s+([0-9\.]+)\s+([0-9\.]+).*")
XML_NAMESPACE = "http://psi.hupo.org/ms/mzml"
SPECTRA = re.compile("^\S[A-Ya-y0-9\s\.\+]+Z\s[0-9]+\s([0-9\.e\+]+)\n+([0-9\.\se\+]+)", re.MULTILINE)
SPECTRA_START = re.compile("S\s")
#  spectra-<timestamp>-<nonce>-<file-id>-<split>-<id>-<max-id>.<ext>
FILE_FORMAT = "spectra-{0:f}-{1:d}-{2:d}-{3:d}-{4:d}-{5:d}.{6:s}"
FILE_REGEX = re.compile("spectra-([0-9\.]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)\.([A-Za-z]+)")

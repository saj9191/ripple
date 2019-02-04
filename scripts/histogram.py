import matplotlib
import xml.etree.ElementTree as ET
matplotlib.use('Agg')
import matplotlib.pyplot as plt

file_name = "TN_CSF_062617_01.mzML"


def get_confidence_values():
  f = open("test1/percolator/percolator.target.psms.txt")
  scan_index = 1
  qvalue_index = 7
  lines = f.readlines()[1:]

  scan_to_confidence = {}

  for line in lines:
    parts = line.split("\t")
    confidence = float(parts[qvalue_index])
    scan = int(parts[scan_index])
    scan_to_confidence[scan] = confidence

  return scan_to_confidence


def cvParam(spectrum, name):
  for cvParam in spectrum:
    if cvParam.get("name") == name:
      return float(cvParam.get("value"))
  raise Exception("")


def getId(spectrum):
  return int(spectrum.get("id").split("=")[-1])


bin_size = 10000
bin_to_count = {}
not_found = 0
total = 0
confident_count = 0


confidence_values = get_confidence_values()
tree = ET.parse("test1/{0:s}".format(file_name))
spectra = tree.getroot()[0][-1][0]
print("1", len(spectra))
spectra = list(filter(lambda s: cvParam(s, "ms level") == 2.0, spectra))
print("2", len(spectra))
spectra = list(map(lambda s: [-1 * cvParam(s, "total ion current"), getId(s)], spectra))
print("3", len(spectra))
total = len(spectra)
spectra.sort()

print("spectra", len(spectra))
num_bins = int((len(spectra) + bin_size - 1) / bin_size)
print("num bins", num_bins)
print("first", spectra[0])
print("last", spectra[-1])

for i in range(num_bins):
  subset_spectra = spectra[i * bin_size: (i + 1) * bin_size]
  bin_to_count[i] = [0, 0]
  for spectrum in subset_spectra:
    bin_to_count[i][1] += 1
    if spectrum[1] in confidence_values:
      if confidence_values[spectrum[1]] <= 0.01:
        confident_count += 1
        bin_to_count[i][0] += 1
    else:
      not_found += 1

print("Confident", confident_count, "Not Found", not_found, "Total", total)

bins = []
match = []
not_match = []
ticks = []
for i in range(num_bins):
  bins.append(i)
  ticks.append((i + 1) * bin_size)
  if i in bin_to_count:
    match.append(bin_to_count[i][0])
    not_match.append(bin_to_count[i][1] - bin_to_count[i][0])
  else:
    match.append(0)
    not_match.append(0)

print("bins", bins)
print("matches", match)

legends = []
labels = []
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.set_yscale("log")
p = ax.bar(bins, match)
labels.append("Matches")
legends.append(p[0])
p = ax.bar(bins, not_match, bottom=match)
labels.append("Non-Matches")
legends.append(p[0])
plt.xlabel("Spectra Range")
plt.ylabel("Count")
plt.xticks(range(num_bins), ticks)
plt.title(file_name)
fig.legend(legends, labels, loc="upper left")
fig.savefig("histogram_" + file_name + ".png")
plt.close()

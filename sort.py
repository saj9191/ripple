import argparse
import constants


def sort(content):
  it = constants.SPECTRA.finditer(content)
  spectra = list(map(lambda m: (float(m.group(1)), m.group(0)), it))
  spectra = sorted(spectra, key=lambda spectrum: spectrum[0])
  return spectra


def run(args):
  f = open(args.file)
  content = f.read()
  f.close()

  spectra = sort(content)

  sorted_file = "sorted_{0:s}".format(args.file)
  f = open(sorted_file, "w+")

  for spectrum in spectra:
    f.write(spectrum[1])
  f.close()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--file", type=str, required=True, help="Spectra file to sort")
  args = parser.parse_args()
  run(args)


if __name__ == "__main__":
  main()

import time
import unittest
import util

s = """S	35078	35078	934.11
I	RTime	76.2596
I	MS1Intensity	949660.00
Z	3	2800.325347
197.1282 17453
S	32711	32711	934.12
I	RTime	72.4277
I	MS1Intensity	592330.00
Z	3	2800.360147
199.1808 2663
201.1223 2670
S	44458	44458	934.41
I	RTime	91.4918
I	MS1Intensity	0.00
Z	3	2801.212147
200.1880 1702
"""

class UtilTest(unittest.TestCase):
  def test_get_next_spectra(self):
    lines = s.strip().split("\n")
    start_index = 0

    [mass, spectra, start_index] = util.get_next_spectra(lines, start_index)
    self.assertEqual(5, start_index)
    self.assertEqual(2800.325347, mass)
    self.assertEqual(spectra.strip(), "\n".join(lines[0:5]))
    [mass, spectra, start_index] = util.get_next_spectra(lines, start_index)
    self.assertEqual(11, start_index)
    self.assertEqual(2800.360147, mass)
    self.assertEqual(spectra.strip(), "\n".join(lines[5:11]))

    [mass, spectra, start_index] = util.get_next_spectra(lines, start_index)
    self.assertEqual(-1, start_index)
    self.assertEqual(2801.212147, mass)
    self.assertEqual(spectra.strip(), "\n".join(lines[11:]))

  def test_file_name(self):
    now = time.time()
    file_name = util.file_name(now, 1, 2, 3, "txt")
    self.assertEqual(file_name, "spectra-{0:f}-1-2-3.txt".format(now))

  def test_parse_spectra(self):
    [spectra, remainder] = util.parse_spectra(s)

    self.assertEqual(len(spectra), 2)

    expected_remainder = "\n".join(s.split("\n")[11:])
    self.assertEqual(expected_remainder, remainder)

if __name__ == "__main__":
  unittest.main()



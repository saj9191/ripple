import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class Tide(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("tide/basic-tide.json")

    files = ["crux"]
    for file in ["auxlocs", "fasta", "pepix", "protix"]:
      files.append("normalHuman/{0:s}".format(file))

    pp.populate_table("maccoss-fasta", "tide/", files)

    name = "0/123.400000-13/1-1/1-0.000000-1-tide.mzML"
    print("Running pipeline...")
    pp.run(name, "tide/tide.mzML")
    print("Finished. Analyzing output...")

    with open(pp.dir_path + "/tide/tide-search.txt") as f:
      expected_output: List[str] = sorted(f.read().split("\n"))

    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
    entry: TestEntry = entries[-2]
    actual_output: List[str] = sorted(entry.get_content().decode("utf-8").split("\n"))
    print("Checking Tide counts equal...")
    self.assertEqual(len(expected_output), len(actual_output))
    print("Comparing Tide output ({0:d} lines)...".format(len(expected_output)))
    self.assertListEqual(expected_output, actual_output)

    print("Comparing Percolator output...")
    with open(pp.dir_path + "/tide/percolator.target.peptides.txt") as f:
      expected_output: List[str] = sorted(f.read().split("\n"))

    entry: TestEntry = entries[-1]
    actual_output: List[str] = sorted(entry.get_content().decode("utf-8").split("\n"))
    self.assertLessEqual(float(abs(len(expected_output) - len(actual_output)))/len(expected_output), 0.05)
    print("Cleaning up...")
    pp.database.destroy()


if __name__ == "__main__":
    unittest.main()

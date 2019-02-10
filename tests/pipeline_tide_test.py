import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class Tide(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("tide/basic-tide.json")

    files = ["tide/crux"]
    for file in ["auxlocs", "fasta", "pepix", "protix"]:
      files.append("tide/normalHuman/{0:s}".format(file))

    pp.populate_table("maccoss-fasta", "tide/", files)

    name = "0/123.400000-13/1-1/1-1-1-tide.mzML"
    with open("tide/tide.mzML") as f:
      pp.run(name, f.read())

    with open("tide/tide-search.txt") as f:
      expected_output: List[str] = sorted(f.read().split("\n"))

    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
    entry: TestEntry = entries[-2]
    actual_output: List[str] = sorted(entry.get_content().split("\n"))
    self.assertCountEqual(expected_output, actual_output)
    self.assertListEqual(expected_output, actual_output)

    with open("tide/percolator.target.peptides.txt") as f:
      expected_output: List[str] = sorted(f.read().split("\n"))

    entry: TestEntry = entries[-1]
    actual_output: List[str] = sorted(entry.get_content().split("\n"))
    self.assertLessEqual(abs(len(expected_output) - len(actual_output)), 50)


if __name__ == "__main__":
    unittest.main()

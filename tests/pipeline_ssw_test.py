import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class SmithWaterman(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("ssw/smith-waterman.json")

    files = []
    for i in range(3):
      files.append("uniprot-fasta-{0:d}".format(i + 1))

    pp.populate_table("ssw-database", "ssw/", files)
    pp.populate_table("ssw-program", "ssw/", ["ssw_test"])

    name = "0/123.400000-13/1-1/1-1-1-fasta.fasta"
    with open("ssw/input-10.fasta") as f:
      pp.run(name, f.read())

    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
    entry: TestEntry = entries[-1]
    actual_output: List[str] = filter(lambda item: len(item.strip()) > 0, entry.get_content().split("\n\n"))
    blast = pp.__import_format__("blast")
    actual_output = sorted(actual_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])

    with open("ssw/output") as f:
      expected_output: List[str] = list(filter(lambda item: len(item.strip()) > 0, f.read().split("\n\n")))
    expected_output = sorted(expected_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])

    self.assertCountEqual(actual_output, expected_output)


if __name__ == "__main__":
    unittest.main()

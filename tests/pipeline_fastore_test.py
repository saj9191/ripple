import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict
import os


class FaStore(unittest.TestCase):
  def test_basic(self):
    directory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(directory)
    
    pp: pipeline.Pipeline = pipeline.Pipeline("fastore/fastore_compression.json")

    files = ["fastore_bin", "fastore_rebin", "fastore_pack","fastore_compress.sh"]

    pp.populate_table("jessie-fastore-program", "fastore/", files)

    name = "0/123.400000-13/1-1/1-1-1-fasta.fq"
    pp.run(name, "fastore/SP1.fq")

#    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
#    entry: TestEntry = entries[-1]
#    actual_output: List[str] = filter(lambda item: len(item.strip()) > 0, entry.get_content().split("\n\n"))
#    blast = pp.__import_format__("blast")
#    actual_output = sorted(actual_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])
#
#    with open(pp.dir_path + "/ssw/output") as f:
#      expected_output: List[str] = list(filter(lambda item: len(item.strip()) > 0, f.read().split("\n\n")))
#    expected_output = sorted(expected_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])
#
#    self.assertCountEqual(actual_output, expected_output)
#    pp.database.destroy()


if __name__ == "__main__":
    unittest.main()

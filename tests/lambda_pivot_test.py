import blast
import pivot_file
import tutils
import unittest
from tutils import TestDatabase, TestEntry, TestTable


class PivotMethods(unittest.TestCase):
  def test_basic(self):
    params = {
      "bucket": "table1",
      "file": "sort",
      "input_format": "blast",
      "identifier": "score",
      "log": "log",
      "name": "sort",
      "num_pivot_bins": 2,
      "timeout": 60,
    }

    database: TestDatabase = TestDatabase(params)
    log: TestTable = database.create_table(params["log"])
    table1: TestTable = database.create_table(params["bucket"])
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-0.0000-1-suffix.blast",
"""target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 2 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 290 suboptimal_alignment_score: 321

target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48""")

    event = tutils.create_event(database, table1.name, entry1.key)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 2)
    self.assertEqual(entries[1].get_content().decode("utf-8"), "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, entry1.key))

  def test_offsets(self):
    params = {
      "bucket": "table1",
      "file": "sort",
      "input_format": "blast",
      "identifier": "score",
      "log": "log",
      "name": "sort",
      "num_pivot_bins": 2,
      "storage_class": "STANDARD",
      "timeout": 60,
    }

    database: TestDatabase = TestDatabase(params)
    log: TestTable = database.create_table(params["log"])
    table1: TestTable = database.create_table(params["bucket"])
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-0.0000-1-suffix.blast",
"""target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 2 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 290 suboptimal_alignment_score: 321

target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48""")
    event = tutils.create_event(database, table1.name, entry1.key)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 2)
    self.assertEqual(entries[1].get_content().decode("utf-8"), "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, entry1.key))


if __name__ == "__main__":
  unittest.main()

import inspect
import os
import sys
import unittest
import tutils
from tutils import TestDatabase, TestTable

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import combine_files
sys.path.insert(0, parentdir + "/formats")


class CombineFunction(unittest.TestCase):
  def test_basic(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    table1.add_entry("0/123.400000-13/1-1/2-1-2-suffix.new", "G H I\nJ K L\n")
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-1-2-suffix.new", "A B C\nD E F\n")
    log = database.create_table("log")
    params = {
      "batch_size": 2,
      "bucket": table1.name,
      "chunk_size": 20,
      "file": "combine_file",
      "format": "new_line",
      "log": log.name,
      "name": "combine",
      "ranges": False,
      "sort": False,
      "storage_class": "test",
      "timeout": 60,
    }

    event = tutils.create_event(database, table1.name, entry1.key, params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)
    entries: List[TestEntry] = database.get_entries(table1.name)
    self.assertEqual(len(entries), 3)
    combined_entry = entries[-1]
    self.assertEqual(combined_entry.key, "1/123.400000-13/1-1/1-1-1-suffix.new")
    self.assertEqual(combined_entry.content, "A B C\nD E F\nG H I\nJ K L\n")

  def test_batches(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable  = database.create_table("table1")
    log: TestTable = database.create_table("log")
    params = {
      "batch_size": 2,
      "bucket": table1.name,
      "chunk_size": 20,
      "file": "combine_file",
      "format": "new_line",
      "log": log.name,
      "name": "combine",
      "ranges": False,
      "sort": False,
      "storage_class": "test",
      "timeout": 60,
    }

    table1.add_entry("0/123.400000-13/1-1/1-1-3-suffix.new", "A B C\nD E F\n")
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/3-1-3-suffix.new", "M N O\bP Q R\n")
    entry2: TestEntry = table1.add_entry("0/123.400000-13/1-1/2-1-3-suffix.new", "G H I\nJ K L\n")
    entries: List[TestEntry] = database.get_entries(table1.name)
    event = tutils.create_event(database, table1.name, entry1.key, params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)

    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 4)
    combined_entry = entries[-1]
    self.assertEqual(combined_entry.key, "1/123.400000-13/1-1/2-1-2-suffix.new")
    self.assertEqual(combined_entry.get_content(), "M N O\bP Q R\n")

    event = tutils.create_event(database, table1.name, entry2.key, params)
    combine_files.handler(event, context)
    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 5)
    combined_entry = entries[-2]
    self.assertEqual(combined_entry.key, "1/123.400000-13/1-1/1-1-2-suffix.new")
    self.assertEqual(combined_entry.get_content(), "A B C\nD E F\nG H I\nJ K L\n")


if __name__ == "__main__":
  unittest.main()

import combine_files
import unittest
import tutils
from tutils import TestDatabase, TestTable


class CombineFunction(unittest.TestCase):
  def test_basic(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    table1.add_entry("0/123.400000-13/1-1/2-0.00000-2-suffix.new", "G H I\nJ K L\n")
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-0.0000-2-suffix.new", "A B C\nD E F\n")
    log = database.create_table("log")
    params = {
      "bucket": table1.name,
      "chunk_size": 20,
      "file": "combine_file",
      "format": "new_line",
      "log": log.name,
      "name": "combine",
      "ranges": False,
      "sort": False,
      "output_format": "new_line", 
      "timeout": 60,
    }
    database.params = params
    event = tutils.create_event(database, table1.name, entry1.key)
    context = tutils.create_context(params)
    combine_files.handler(event, context)
    entries: List[TestEntry] = database.get_entries(table1.name)
    self.assertEqual(len(entries), 3)
    combined_entry = entries[-1]
    self.assertEqual(combined_entry.key, "1/123.400000-13/1-1/1-0.000000-1-suffix.new")
    self.assertEqual(combined_entry.get_content().decode("utf-8"), "A B C\nD E F\nG H I\nJ K L\n")


if __name__ == "__main__":
  unittest.main()

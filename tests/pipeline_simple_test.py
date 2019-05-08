import os
import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class Simple(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("../json/simple.json")

    with open("test.txt", "w+")  as f:
      f.write("hello world")

    name = "0/123.400000-13/1-1/1-0.000000-1-tide.txt"
    pp.run(name, "test.txt")

    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
    assert(len(entries) == 2)

    input = entries[0].get_content()
    output = entries[1].get_content()
    self.assertEqual(input, output)
    pp.database.destroy()
    os.remove("test.txt")


if __name__ == "__main__":
    unittest.main()

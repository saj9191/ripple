import inspect
import os
import sys
import unittest
import tutils
from tutils import TestDatabase, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import combine_files
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\n"
object1 = Object("0/123.400000-13/1-1/1-1-2-suffix.new", content, 2)
content = "G H I\nJ K L\n"
object2 = Object("0/123.400000-13/1-1/2-1-2-suffix.new", content, 1)

content = "A B C\nD E F\n"
object4 = Object("0/123.400000-13/1-1/1-1-3-suffix.new", content, 0)
content = "G H I\nJ K L\n"
object5 = Object("0/123.400000-13/1-1/2-1-3-suffix.new", content, 2)
content = "M N O\bP Q R\n"
object6 = Object("0/123.400000-13/1-1/3-1-3-suffix.new", content, 1)

params = {
  "batch_size": 2,
  "chunk_size": 20,
  "file": "combine_file",
  "format": "new_line",
  "log": "log",
  "name": "combine",
  "ranges": False,
  "sort": False,
  "storage_class": "test",
  "timeout": 60,
}


class CombineFunction(unittest.TestCase):
  def test_basic(self):
    s3 = TestDatabase()
    table1 = s3.create_table("table1")
    table1.add_objects([object1, object2])
    s3.create_table("log")
    params["bucket"] = table1.name
    event = tutils.create_event(s3, table1.name, object1.key, params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)
    objs = sorted(s3.get_objects(table1.name), key=lambda obj: obj.key)
    self.assertEqual(len(objs), 3)
    combined_obj = objs[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/1-1-1-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")

  def test_batches(self):
    s3 = TestDatabase()
    table1 = s3.create_table("table1")
    s3.create_table("log")
    params["bucket"] = table1.name
    table1.add_objects([object4, object5, object6])
    event = tutils.create_event(s3, table1.name, object6.key, params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)

    objs = sorted(s3.get_objects(table1.name), key=lambda obj: obj.key)
    self.assertEqual(len(objs), 4)
    combined_obj = objs[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/2-1-2-suffix.new")
    self.assertEqual(combined_obj.content, "M N O\bP Q R\n")

    event = tutils.create_event(s3, table1.name, object5.key, params)
    combine_files.handler(event, context)
    objs = sorted(s3.get_objects(table1.name), key=lambda obj: obj.key)
    self.assertEqual(len(objs), 5)
    combined_obj = objs[-2]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/1-1-2-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")


if __name__ == "__main__":
  unittest.main()

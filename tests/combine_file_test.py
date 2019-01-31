import inspect
import os
import sys
import unittest
import tutils
from tutils import Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import combine_files
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\n"
object1 = Object("0/123.400000-13/1-1/1-1-2-suffix.new", content, 2)
content = "G H I\nJ K L\n"
object2 = Object("0/123.400000-13/1-1/2-1-2-suffix.new", content, 1)
bucket1 = Bucket("bucket1", [object1, object2])

content = "A B C\nD E F\n"
object4 = Object("0/123.400000-13/1-1/1-1-3-suffix.new", content, 0)
content = "G H I\nJ K L\n"
object5 = Object("0/123.400000-13/1-1/2-1-3-suffix.new", content, 2)
content = "M N O\bP Q R\n"
object6 = Object("0/123.400000-13/1-1/3-1-3-suffix.new", content, 1)
bucket2 = Bucket("bucket2", [object4, object5, object6])

log = Bucket("log", [])

params = {
  "batch_size": 2,
  "bucket": bucket1.name,
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
    event = tutils.create_event(bucket1.name, object1.key, [bucket1, bucket2, log], params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)
    self.assertEqual(len(bucket1.objects.objects), 3)
    combined_obj = bucket1.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/1-1-1-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")

  def test_batches(self):
    params["bucket"] = bucket2.name
    event = tutils.create_event(bucket2.name, object6.key, [bucket1, bucket2, log], params)
    context = tutils.create_context(params)
    combine_files.handler(event, context)

    self.assertEqual(len(bucket2.objects.objects), 4)
    combined_obj = bucket2.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/2-1-2-suffix.new")
    self.assertEqual(combined_obj.content, "M N O\bP Q R\n")

    event = tutils.create_event(bucket2.name, object5.key, [bucket1, bucket2, log], params)
    combine_files.handler(event, context)
    self.assertEqual(len(bucket2.objects.objects), 5)
    combined_obj = bucket2.objects.objects[-1]
    self.assertEqual(combined_obj.key, "1/123.400000-13/1-1/1-1-2-suffix.new")
    self.assertEqual(combined_obj.content, "A B C\nD E F\nG H I\nJ K L\n")


if __name__ == "__main__":
  unittest.main()

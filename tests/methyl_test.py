import inspect
import os
import sys
import unittest
import tutils
from unittest.mock import MagicMock
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import methyl

class IteratorMethods(unittest.TestCase):
  def test_next_offsets(self):
    obj = Object("test.methyl", "A B C\na b c\n1 2 3\n")

    # Read everything in one pass
    it = methyl.Iterator(obj, {}, 2, 20)
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 17)

    # Requires multiple passes
    it = methyl.Iterator(obj, {}, 1, 10)
    [offsets, more] = it.nextOffsets()
    self.assertTrue(more)
    self.assertEqual(offsets["offsets"][0], 0)
    self.assertEqual(offsets["offsets"][1], 5)

    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 6)
    self.assertEqual(offsets["offsets"][1], 17)

  def test_next(self):
    obj = Object("test.methyl", "A B C\na b c\n1 2 3\n")

    # Requires multiple passes
    it = methyl.Iterator(obj, {}, 1, 10)
    [o, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(o, ["A B C", ""])

    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["a b c", "1 2 3", ""])

    # Read everything in one pass
    it = methyl.Iterator(obj, {}, 2, 20)
    [o, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(o, ["A B C", "a b c", "1 2 3", ""])

  def test_combine(self):
    object1 = Object("test1.methyl", "A B C\na b c\n1 2 3\n")
    object2 = Object("test2.methyl", "D E F\nd e f\n4 5 6\n")
    object3 = Object("test3.methyl", "G H I\ng h i\n7 8 9\n")
    object4 = Object("test4.methyl", "J K L\nj k l\n10 11 12\n")
    objects = [object1, object2, object3, object4]
    bucket1 = Bucket("bucket1", objects)
    s3 = S3([bucket1])
    params = {
      "s3": s3,
      "batch_size": 1,
      "chunk_size": 10,
      "identifier": "",
      "sort": False,
    }

    keys = list(map(lambda o: o.name, objects))
    temp_name = "/tmp/ripple_test"
    methyl.Iterator.combine("bucket1", keys, temp_name, params)
    with open(temp_name) as f:
      self.assertEqual(f.read(), object1.content + object2.content + object3.content + object4.content)
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

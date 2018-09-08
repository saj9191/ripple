import inspect
import os
import sys
import unittest
import tutils
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util


class FileNameMethods(unittest.TestCase):
  def test_file_name_parser(self):
    m = {
      "prefix": 0,
      "timestamp": 123.4,
      "nonce": 42,
      "bin": 12,
      "file_id": 3,
      "last": False,
      "suffix": "hello",
      "ext": "txt"
    }
    self.assertDictEqual(m, util.parse_file_name(util.file_name(m)))


class ObjectsMethods(unittest.TestCase):
  object1 = Object("A-object1")
  object2 = Object("B-object1")
  object3 = Object("A-object2")
  object4 = Object("B-object2")
  object5 = Object("A-object3")
  object6 = Object("B-object4")
  bucket1 = Bucket("bucket1", [object1, object2, object3, object4])
  bucket2 = Bucket("bucket2", [object5, object6])
  s3 = S3([bucket1, bucket2])
  params = {
    "s3": s3
  }

  def test_get_objects(self):
    objects = util.get_objects("bucket1", prefix=None, params=self.params)
    self.assertEqual(len(objects), 4)
    self.assertTrue(tutils.equal_lists(objects, [
      self.object1,
      self.object2,
      self.object3,
      self.object4
    ]))

    objects = util.get_objects("bucket1", prefix="A", params=self.params)
    self.assertEqual(len(objects), 2)
    self.assertTrue(tutils.equal_lists(objects, [
      self.object1,
      self.object3,
    ]))


if __name__ == "__main__":
  unittest.main()

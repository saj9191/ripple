import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import S3, Bucket, Object
from typing import Any, ClassVar, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import pivot

class TestIterator(pivot.Iterator):
  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds], increment: int):
    pivot.Iterator.__init__(self, obj, offset_bounds)
    self.__class__.increment = increment


class PivotMethods(unittest.TestCase):
  def test_get_pivot_ranges(self):
    content = "bucket_name\nfile_name\n10\t15\t23\t37\t40"
    object1 = Object("pivot.pivot", content)
    bucket1 = Bucket("bucket1", [object1])
    s3 = S3([bucket1])
    params = {
      "test": True,
      "s3": s3,
    }

    [file_bucket, file_key, ranges] = pivot.get_pivot_ranges(bucket1.name, object1.key, params)
    self.assertEqual(file_bucket, "bucket_name")
    self.assertEqual(file_key, "file_name")
    expected_ranges = [{
      "range": [10, 15],
      "bin": 1,
    }, {
      "range": [15, 23],
      "bin": 2,
    }, {
      "range": [23, 37],
      "bin": 3,
    }, {
      "range": [37, 40],
      "bin": 4,
    }]
    self.assertEqual(ranges, expected_ranges)

  def test_combine(self):
    object1 = Object("test1.pivot", "bucket1\nkey1\n20\t25\t60\t61\t80")
    object2 = Object("test2.pivot", "bucket1\nkey2\n1\t40\t50\t63\t81")
    object3 = Object("test3.pivot", "bucket1\nkey3\n10\t12\t40\t41\t42")

    objects = [object1, object2, object3]
    temp_name = "/tmp/ripple_test"
    # 1 10 12 20 25 40 40 41 42 50 60 61 63 80 81
    # *             *              *           *
    it = TestIterator(object1, None, increment=7)
    with open(temp_name, "wb+") as f:
      it.combine(objects, f)

    with open(temp_name) as f:
      self.assertEqual(f.read(), "bucket1\nkey3\n1.0\t40.0\t60.0\t81.0")

    # 1 10 12 20 25 40 40 41 42 50 60 61 63 80 81
    # *                                        *
    with open(temp_name, "wb+") as f:
      pivot.Iterator.combine(objects, f)

    with open(temp_name) as f:
      self.assertEqual(f.read(), "bucket1\nkey3\n1.0\t81.0")
    os.remove(temp_name)

  def test_combine_edge_case(self):
    # If we always increment by an integer amount, we may run into the
    # case where the last bin has significantly less values than the rest
    # of the bins. We want to make sure values are distributed across
    # bins as uniformly as possible, so we increment by non-integer values.
    # This test tests the non-uniform case.
    object1 = Object("test1.pivot", "bucket1\nkey1\n20\t25\t60\t61\t80")
    object2 = Object("test2.pivot", "bucket1\nkey2\n1\t40\t50\t63\t81")
    object3 = Object("test3.pivot", "bucket1\nkey3\n10\t12\t40\t41\t42")
    objects = [object1, object2, object3]

    # 1 10 12 20 25 40 40 41 42 50 60 61 63 80 81
    # *          *           *        *        *
    temp_name = "/tmp/ripple_test"
    it = TestIterator(object1, None, increment=4)
    with open(temp_name, "wb+") as f:
      it.combine(objects, f)

    with open(temp_name) as f:
      self.assertEqual(f.read(), "bucket1\nkey3\n1.0\t25.0\t42.0\t61.0\t81.0")
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

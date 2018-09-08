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
import pivot

class PivotMethods(unittest.TestCase):
  def test_get_pivot_rangess(self):
    content = "bucket_name\nfile_name\n10\t15\t23\t37\t40"
    object1 = Object("pivot.pivot", content)
    bucket1 = Bucket("bucket1", [object1])
    s3 = S3([bucket1])
    params = {
      "test": True,
      "s3": s3,
    }

    [file_bucket, file_key, ranges] = pivot.get_pivot_ranges(bucket1.name, object1.name, params)
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


if __name__ == "__main__":
  unittest.main()

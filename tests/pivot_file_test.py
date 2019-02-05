import inspect
import os
import sys
import tutils
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import pivot_file
sys.path.insert(0, parentdir + "/formats")
import blast


class PivotMethods(unittest.TestCase):
  def test_basic(self):
    log = Bucket("log", [])
    object1 = Object("0/123.400000-13/1-1/1-1-1-suffix.blast",
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
    bucket1 = Bucket("bucket1", [object1])
    s3 = S3([bucket1])

    params = {
      "bucket": "bucket1",
      "file": "sort",
      "format": "blast",
      "identifier": "score",
      "log": "log",
      "name": "sort",
      "num_bins": 2,
      "s3": s3,
      "storage_class": "STANDARD",
      "timeout": 60,
    }

    event = tutils.create_event(bucket1.name, object1.key, [bucket1, log], params)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    objs = bucket1.objects.objects
    self.assertEqual(len(objs), 2)
    self.assertEqual(objs[1].content, "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format("bucket1", object1.key))

if __name__ == "__main__":
  unittest.main()

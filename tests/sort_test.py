import inspect
import os
import sys
import tutils
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import sort
sys.path.insert(0, parentdir + "/formats")
import blast


class SortMethods(unittest.TestCase):
  def test_basic(self):
    log = Bucket("log", [])
    object1 = Object("0/123.400000-13/1-1/1-1-1-suffix.blast",
"""target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48""")
    bucket1 = Bucket("bucket1", [object1])
    s3 = S3([bucket1])
    pivots = []
    increment = 300000
    for i in range(3):
      start = i * increment
      pivots.append({
        "range": [start, start + increment],
        "bin": i + 1
      })

    params = {
      "bucket": "bucket1",
      "file": "sort",
      "format": "blast",
      "identifier": "score",
      "log": "log",
      "name": "sort",
      "pivots": pivots,
      "s3": s3,
      "storage_class": "STANDARD",
      "timeout": 60,
    }

    event = tutils.create_event(bucket1.name, object1.key, [bucket1, log], params)
    context = tutils.create_context(params)
    sort.handler(event, context)
    self.assertEqual(len(bucket1.objects.objects), 4)
    objs = sorted(bucket1.objects.objects, key=lambda obj: obj.key)

    self.assertEqual(objs[1].content,
"""target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48

""")

    self.assertEqual(objs[2].content,
"""target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

""")

    print(objs[3].content)
    self.assertEqual(objs[3].content, "")


if __name__ == "__main__":
  unittest.main()

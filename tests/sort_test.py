import inspect
import os
import sys
import tutils
import unittest
from tutils import TestDatabase, TestTable, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import sort
sys.path.insert(0, parentdir + "/formats")
import blast


class SortMethods(unittest.TestCase):
  def test_basic(self):
    s3 = TestDatabase()
    s3.create_table("log")
    bucket1 = s3.create_table("bucket1")

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
    bucket1.add_object(object1)
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

    event = tutils.create_event(s3, bucket1.name, object1.key, params)
    context = tutils.create_context(params)
    sort.handler(event, context)

    objs = s3.get_objects(bucket1.name)
    self.assertEqual(len(objs), 4)
    objs = sorted(objs, key=lambda obj: obj.key)

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

  def test_offsets(self):
    s3 = TestDatabase()
    s3.create_table("log")
    table1 = s3.create_table("table1")

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
    table1.add_object(object1)
    pivots = []
    increment = 300000
    for i in range(3):
      start = i * increment
      pivots.append({
        "range": [start, start + increment],
        "bin": i + 1
      })

    params = {
      "bucket": table1.name,
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
    event = tutils.create_event(s3, table1.name, object1.key, params)
    context = tutils.create_context(params)
    sort.handler(event, context)
    objs = sorted(s3.get_objects(table1.name, "1/"), key=lambda obj: obj.key)
    self.assertEqual(len(objs), 3)
    self.assertEqual(objs[0].content, """target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48

""")

    self.assertEqual(objs[1].content, """target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

""")

    self.assertEqual(objs[2].content, "")


if __name__ == "__main__":
  unittest.main()

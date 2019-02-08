import inspect
import os
import sys
import tutils
import unittest
from tutils import TestDatabase, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import pivot_file
sys.path.insert(0, parentdir + "/formats")
import blast


class PivotMethods(unittest.TestCase):
  def test_basic(self):
    s3 = TestDatabase()
    s3.create_table("log")
    table1 = s3.create_table("table1")
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
    table1.add_object(object1)

    params = {
      "bucket": table1.name,
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

    event = tutils.create_event(s3, table1.name, object1.key, params)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    objs = s3.get_objects(table1.name)
    self.assertEqual(len(objs), 2)
    self.assertEqual(objs[1].content, "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, object1.key))

  def test_offsets(self):
    s3 = TestDatabase()
    s3.create_table("log")
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
    table1 = s3.create_table("table1")
    table1.add_object(object1)

    params = {
      "bucket": table1.name,
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

    event = tutils.create_event(s3, table1.name, object1.key, params)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    objs = sorted(s3.get_objects(table1.name), key=lambda obj: obj.key)
    self.assertEqual(len(objs), 2)
    self.assertEqual(objs[1].content, "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, object1.key))


if __name__ == "__main__":
  unittest.main()

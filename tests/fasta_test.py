import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import Object
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import fasta


class TestIterator(fasta.Iterator):
  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    fasta.Iterator.__init__(self, obj, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_next(self):
    obj = Object("test.fasta", ">A\tB\tC\n>a\tb\tc\n>1\t2\t3\n")

    # Read everything in one pass
    it = TestIterator(obj, None, 30, 30)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [">A\tB\tC\n", ">a\tb\tc\n", ">1\t2\t3\n"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 20))
    self.assertFalse(more)

    # Requires multiple passes
    it = TestIterator(obj, None, 8, 8)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [">A\tB\tC\n"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 6))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [">a\tb\tc\n"])
    self.assertEqual(offset_bounds, OffsetBounds(7, 13))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [">1\t2\t3\n"])
    self.assertEqual(offset_bounds, OffsetBounds(14, 20))
    self.assertFalse(more)


if __name__ == "__main__":
  unittest.main()

import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import Bucket, Object
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import new_line


class TestIterator(new_line.Iterator):
  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    new_line.Iterator.__init__(self, obj, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_adjust(self):
    obj = Object("test.new_line", "A B C\na b c\n1 2 3\nD E F\nd e f\n")
    it = TestIterator(obj, OffsetBounds(8, 13), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, ["a b c"])
    self.assertEqual(offset_bounds, OffsetBounds(6, 11))
    self.assertFalse(more)

    # No adjustment needed
    it = TestIterator(obj, OffsetBounds(6, 11), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, ["a b c"])
    self.assertEqual(offset_bounds, OffsetBounds(6, 11))
    self.assertFalse(more)

    # Beginning of content
    it = TestIterator(obj, OffsetBounds(0, 7), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(items, ["A B C"])

    # Beginning of content
    it = TestIterator(obj, OffsetBounds(26, obj.content_length - 1), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(items, ["d e f"])

  def test_next(self):
    obj = Object("test.new_line", "A B C\na b c\n1 2 3\n")

    # Requires multiple passes
    it = TestIterator(obj, None, 11, 11)
    [items, offset_bounds, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(OffsetBounds(0, 11), offset_bounds)
    self.assertEqual(items, ["A B C", "a b c"])

    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(items, ["1 2 3"])

  def test_overflow(self):
    obj = Object("test.new_line", "A B C D E F G H\na b c d e f g h\n1 2 3 4 5 6 7 8 9\n")

    # Requires multiple passes
    it = TestIterator(obj, None, 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [])
    self.assertEqual(offset_bounds, None)
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, ["A B C D E F G H"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 15))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, ["a b c d e f g h"])
    self.assertEqual(offset_bounds, OffsetBounds(16, 31))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, [])
    self.assertEqual(offset_bounds, None)
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(items, ["1 2 3 4 5 6 7 8 9"])
    self.assertEqual(offset_bounds, OffsetBounds(32, 49))
    self.assertFalse(more)

  def test_combine(self):
    object1 = Object("test1.new_line", "A B C\na b c\n1 2 3\n")
    object2 = Object("test2.new_line", "D E F\nd e f\n4 5 6\n")
    object3 = Object("test3.new_line", "G H I\ng h i\n7 8 9\n")
    object4 = Object("test4.new_line", "J K L\nj k l\n10 11 12\n")
    objects = [object1, object2, object3, object4]

    temp_name = "/tmp/ripple_test"
    with open(temp_name, "wb+") as f:
      new_line.Iterator.combine(objects, f)

    with open(temp_name) as f:
      self.assertEqual(f.read(), object1.content + object2.content + object3.content + object4.content)
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()

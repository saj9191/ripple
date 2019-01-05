import inspect
import os
import queue
import sys
import unittest
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import priority_queue


class PriorityQueueTests(unittest.TestCase):
  def test_fifo_queue(self):
    item1 = priority_queue.Item(1, "pre", 1, None, {})
    item2 = priority_queue.Item(10, "post", 2, None, {})

    q = priority_queue.Fifo()
    q.put(item1)
    q.put(item2)

    self.assertEqual(item1, q.get())
    self.assertEqual(item2, q.get())
    self.assertRaises(queue.Empty, q.get)

  def test_robin_queue(self):
    item1 = priority_queue.Item(1, "pre", 1, None, {})
    item2 = priority_queue.Item(10, "post", 2, None, {})
    item3 = priority_queue.Item(11, "what", 1, None, {})
    item4 = priority_queue.Item(2, "huh", 2, None, {})

    q = priority_queue.Robin()
    q.put(item1)
    q.put(item3)
    q.put(item2)
    q.put(item4)

    self.assertEqual(item1, q.get())
    self.assertEqual(item4, q.get())
    self.assertEqual(item3, q.get())
    self.assertEqual(item2, q.get())
    self.assertRaises(queue.Empty, q.get)

  def test_deadline_queue(self):
    item1 = priority_queue.Item(1, "pre", 1, 100, {})
    item2 = priority_queue.Item(10, "post", 2, 5, {})

    q = priority_queue.Deadline()
    q.put(item1)
    q.put(item2)

    self.assertEqual(item2, q.get())
    self.assertEqual(item1, q.get())
    self.assertRaises(queue.Empty, q.get)


if __name__ == "__main__":
  unittest.main()

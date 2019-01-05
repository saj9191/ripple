import inspect
import os
import sys
import unittest
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import scheduler


class MockScheduler(scheduler.Scheduler):
  def __init__(self, policy, params, existing_objects, children_payloads, max_iterations):
    scheduler.Scheduler.__init__(self, policy, params)
    self.children_payloads = children_payloads
    self.existing_objects = existing_objects
    self.max_iterations = max_iterations
    self.current_iteration = 0

  def __aws_connections__(self):
    pass

  def __object_exists__(self, object_key):
    exists = object_key in self.existing_objects
    return exists

  def __get_children_payloads__(self, object_key):
    return self.children_payloads[object_key]

  def __invoke__(self, name, payload):
    pass

  def __running__(self):
    running = self.current_iteration < self.max_iterations
    self.current_iteration += 1
    return running


class SchedulerTests(unittest.TestCase):
  def test_initiation(self):
    mock = MockScheduler("fifo", {}, set(), {}, 1)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 0)

  def test_initiate(self):
    params = {
      "bucket": "bucket",
      "log": "log",
      "functions": {
        "top-mzML": {
          "file": "top"
        }
      },
      "pipeline": [{
        "name": "top-mzML",
      }]
    }
    key = "0/123.400000-13/1/1-1-3-suffix.new"

    mock = MockScheduler("fifo", params, set(), {}, 1)
    payload = mock.__payload__(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 1)
    self.assertEqual(mock.running_tasks[0].output_file, "1/123.400000-13/1/1-1-3-suffix.log")

  def test_finish(self):
    params = {
      "bucket": "bucket",
      "log": "log",
      "functions": {
        "top-mzML": {
          "file": "top"
        }
      },
      "pipeline": [{
        "name": "top-mzML",
      }]
    }
    key = "0/123.400000-13/1/1-1-3-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-3-suffix.log": []
    }

    existing_objects = set(["1/123.400000-13/1/1-1-3-suffix.log"])
    mock = MockScheduler("fifo", params, existing_objects, children_payloads, 2)
    payload = mock.__payload__(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 0)


if __name__ == "__main__":
  unittest.main()

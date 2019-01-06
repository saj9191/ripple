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
    self.num_invokes = 0

  def __aws_connections__(self):
    pass

  def __object_exists__(self, object_key):
    exists = object_key in self.existing_objects
    return exists

  def __get_children_payloads__(self, object_key):
    return self.children_payloads[object_key]

  def __invoke__(self, name, payload):
    self.num_invokes += 1

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
      }],
      "timeout": 600,
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"

    mock = MockScheduler("fifo", params, set(), {}, 1)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 1)
    self.assertEqual(mock.running_tasks[0].output_file, "1/123.400000-13/1/1-1-1-suffix.log")
    self.assertEqual(mock.running_tasks[0].prefix, 0)
    self.assertEqual(mock.num_invokes, 1)

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
      }],
      "timeout": 600,
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-1-suffix.log": []
    }

    existing_objects = set(["1/123.400000-13/1/1-1-1-suffix.log"])
    mock = MockScheduler("fifo", params, existing_objects, children_payloads, 2)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 0)

  def test_step(self):
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
      }, {
        "name": "top-mzML",
      }],
      "timeout": 600,
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-1-suffix.log": [
        scheduler.payload(params["bucket"], "1/123.400000-13/1/1-1-1-suffix.new")
      ],
      "2/123.400000-13/1/1-1-1-suffix.log": [],
    }

    existing_objects = set(["1/123.400000-13/1/1-1-1-suffix.log", "2/123.400000-13/1/1-1-1-suffix.log"])
    mock = MockScheduler("fifo", params, existing_objects, children_payloads, 2)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 1)
    self.assertEqual(mock.running_tasks[0].prefix, 1)

  def test_multiple_children(self):
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
      }, {
        "name": "top-mzML",
      }],
      "timeout": 600,
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-1-suffix.log": [
        scheduler.payload(params["bucket"], "1/123.400000-13/1/1-1-3-suffix.new"),
        scheduler.payload(params["bucket"], "1/123.400000-13/1/2-1-3-suffix.new"),
        scheduler.payload(params["bucket"], "1/123.400000-13/1/3-1-3-suffix.new")
      ],
      "2/123.400000-13/1/1-1-3-suffix.log": [],
      "2/123.400000-13/1/2-1-3-suffix.log": [],
      "2/123.400000-13/1/3-1-3-suffix.log": [],
    }

    existing_objects = set([
      "1/123.400000-13/1/1-1-1-suffix.log",
      "2/123.400000-13/1/1-1-3-suffix.log",
      "2/123.400000-13/1/2-1-3-suffix.log",
      "2/123.400000-13/1/3-1-3-suffix.log"
    ])
    mock = MockScheduler("fifo", params, existing_objects, children_payloads, 2)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 3)
    for task in mock.running_tasks:
      self.assertEqual(task.prefix, 1)

    mock.current_iteration = 0
    mock.max_iterations = 1
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 0)

  def test_retrigger(self):
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
      }],
      "timeout": 0,
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-1-suffix.log": []
    }

    existing_objects = set()
    mock = MockScheduler("fifo", params, existing_objects, children_payloads, 2)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 1)
    self.assertEqual(mock.running_tasks[0].prefix, 0)
    self.assertEqual(mock.num_invokes, 2)


if __name__ == "__main__":
  unittest.main()

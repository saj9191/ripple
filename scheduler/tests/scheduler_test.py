import inspect
import json
import os
import sys
import unittest
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import scheduler


class MockTask(scheduler.Task):
  def __init__(self, objects, children, bucket_name, key, timeout, params):
    scheduler.Task.__init__(self, bucket_name, key, timeout, params)
    self.children = children
    self.num_invokes = 0
    self.objects = objects

  def __get_children_payloads__(self, object_key):
    return self.children[object_key]

  def __get_objects__(self):
    if self.stage not in self.objects:
      return []
    return self.objects[self.stage]

  def __invoke__(self, name, payload):
    self.num_invokes += 1

  def __setup_client__(self):
    pass


class MockScheduler(scheduler.Scheduler):
  def __init__(self, policy, params, timeout, existing_objects, children_payloads, max_iterations):
    scheduler.Scheduler.__init__(self, policy, timeout, params)
    self.children_payloads = children_payloads
    self.current_iteration = 0
    self.existing_objects = existing_objects
    self.max_iterations = max_iterations
    self.objects = {}
    self.children = {}
    self.num_invokes = 0

  def __add_task__(self, key):
    self.tasks.append(MockTask(self.objects, self.children, self.params["log"], key, self.timeout, self.params))

  def __aws_connections__(self):
    pass

  def __delete_message__(self, queue, message):
    pass

  def __get_messages__(self, queue):
    messages = queue["messages"]
    queue["messages"] = []
    return messages

  def __object_exists__(self, object_key):
    exists = object_key in self.existing_objects
    return exists

  def __running__(self):
    running = self.current_iteration < self.max_iterations
    self.current_iteration += 1
    return running

  def __setup_sqs_queues__(self):
    self.log_queue = {"messages": []}

  def add_to_queue(self, queue, payload):
    queue["messages"].append({"Body": json.dumps(payload)})


class SchedulerTests(unittest.TestCase):
  def test_initiation(self):
    mock = MockScheduler("fifo", {}, 60, set(), {}, 1)
    mock.listen()
    self.assertEqual(len(mock.tasks), 0)

  def test_simple(self):
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

    key = "0/123.400000-13/1-1/1-1-1-suffix.log"
    mock = MockScheduler("fifo", params, 600, set(), {}, 1)
    mock.objects[0] = [key]
    mock.children[key] = []
    payload = scheduler.payload(params["bucket"], key)
    mock.add_to_queue(mock.log_queue, payload)
    mock.listen()
    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(mock.tasks[0].num_invokes, 0)
    self.assertFalse(mock.tasks[0].running)

  def test_steps(self):
    params = {
      "bucket": "bucket",
      "log": "log",
      "functions": {
        "step": {
          "file": "top"
        }
      },
      "pipeline": [{
        "name": "step",
      }, {
        "name": "step",
      }, {
        "name": "step",
      }]
    }
    key = "0/123.400000-13/1-1/1-1-1-suffix.log"
    mock = MockScheduler("fifo", params, 600, set(), {}, 1)
    mock.add_to_queue(mock.log_queue, scheduler.payload(params["bucket"], key))
    mock.objects[0] = [key]

    children_keys = ["1/123.400000-13/1-1/1-1-2-suffix.new", "1/123.400000-13/1-1/2-1-2-suffix.new"]
    mock.children[key] = []
    for k in children_keys:
      mock.children[key].append(scheduler.payload(params["bucket"], k))
      mock.children[k] = []
    mock.objects[1] = children_keys

    grandchildren_keys = ["2/123.400000-13/1-1/1-1-2-suffix.new", "2/123.400000-13/1-1/2-1-2-suffix.new"]
    for i in range(len(children_keys)):
      mock.children[children_keys[i]].append(scheduler.payload(params["bucket"], grandchildren_keys[i]))
      mock.children[grandchildren_keys[i]] = []
    mock.objects[2] = grandchildren_keys

    mock.listen()
    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(mock.tasks[0].num_invokes, 0)
    self.assertFalse(mock.tasks[0].running)

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
      }]
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
    mock = MockScheduler("fifo", params, 600, existing_objects, children_payloads, 2)
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
      }]
    }
    key = "0/123.400000-13/1/1-1-1-suffix.new"
    children_payloads = {
      "1/123.400000-13/1/1-1-1-suffix.log": []
    }

    existing_objects = set()
    mock = MockScheduler("fifo", params, 0, existing_objects, children_payloads, 2)
    payload = scheduler.payload(params["bucket"], key)
    mock.add(1, None, payload)
    mock.listen()
    self.assertEqual(len(mock.running_tasks), 1)
    self.assertEqual(mock.running_tasks[0].prefix, 0)
    self.assertEqual(mock.num_invokes, 2)


if __name__ == "__main__":
  unittest.main()

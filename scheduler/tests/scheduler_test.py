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
  def __init__(self, bucket_name, token, messages, timeout, params, objects, max_iterations):
    scheduler.Task.__init__(self, bucket_name, token, messages, timeout, params)
    self.current_iteration = 0
    self.max_iterations = max_iterations
    self.num_invokes = 0
    self.objects = objects

  def __get_object__(self, key):
    return self.objects[key]

  def __invoke__(self, name, payload):
    self.num_invokes += 1

  def __running__(self):
    running = self.current_iteration < self.max_iterations
    self.current_iteration += 1
    return running

  def __setup_client__(self):
    pass


class MockScheduler(scheduler.Scheduler):
  def __init__(self, policy, params, timeout, max_iterations):
    scheduler.Scheduler.__init__(self, policy, timeout, params)
    self.current_iteration = 0
    self.max_iterations = max_iterations
    self.objects = {}

  def __add_task__(self, token):
    self.tasks.append(MockTask(self.params["log"], token, self.messages, self.timeout, self.params, self.objects, self.max_iterations))

  def __aws_connections__(self):
    pass

  def __delete_message__(self, queue, message):
    pass

  def __fetch_messages__(self, queue):
    messages = queue["messages"]
    queue["messages"] = []
    return messages

  def __running__(self):
    running = self.current_iteration < self.max_iterations
    self.current_iteration += 1
    return running

  def __setup_sqs_queues__(self):
    self.log_queue = {"messages": []}

  def add_to_queue(self, queue, payload):
    queue["messages"].append({"Body": json.dumps(payload)})

  def add_children_payloads(self, key, payloads):
    self.objects[key] = {"payloads": payloads}


class SchedulerTests(unittest.TestCase):
  def test_initiation(self):
    mock = MockScheduler("fifo", {}, 60, 1)
    mock.listen()
    self.assertEqual(len(mock.tasks), 0)

  def test_simple(self):
    params = {
      "bucket": "bucket",
      "log": "log",
      "functions": {
        "step": {
          "file": "application"
        }
      },
      "pipeline": [{
        "name": "step",
      }]
    }

    key = "0/123.400000-13/1-1/1-1-1-suffix.log"
    mock = MockScheduler("fifo", params, 600, 1)
    mock.add_children_payloads(key, [])
    mock.add_to_queue(mock.log_queue, scheduler.payload(params["bucket"], key))
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
          "file": "application"
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
    mock = MockScheduler("fifo", params, 600, 3)
    mock.add_to_queue(mock.log_queue, scheduler.payload(params["log"], key))
    children = []

    for i in range(3):
      bucket_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      log_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.log".format(i)
      mock.add_to_queue(mock.log_queue, scheduler.payload(params["log"], log_key))
      children.append(scheduler.payload(params["bucket"], bucket_key))

    mock.add_children_payloads(key, children)

    for i in range(3):
      parent_log_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.log".format(i)
      children = []
      for j in range(2):
        log_key = "2/123.400000-13/{1:d}-2/{0:d}-1-3-suffix.log".format(i, j)
        bucket_key = "2/123.400000-13/{1:d}-2/{0:d}-1-3-suffix.new".format(i, j)
        mock.add_to_queue(mock.log_queue, scheduler.payload(params["bucket"], log_key))
        mock.add_children_payloads(log_key, [])
        children.append(scheduler.payload(params["bucket"], bucket_key))
      mock.add_children_payloads(parent_log_key, children)
    mock.listen()

    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(mock.tasks[0].num_invokes, 0)
    self.assertFalse(mock.tasks[0].running)

  def test_retrigger(self):
    params = {
      "bucket": "bucket",
      "log": "log",
      "functions": {
        "step": {
          "file": "application"
        }
      },
      "pipeline": [{
        "name": "step",
      }, {
        "name": "step",
      }]
    }
    key = "0/123.400000-13/1-1/1-1-1-suffix.log"
    mock = MockScheduler("fifo", params, 0, 2)
    mock.add_to_queue(mock.log_queue, scheduler.payload(params["bucket"], key))
    children = []

    for i in range(3):
      bucket_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      log_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.log".format(i)
      children.append(scheduler.payload(params["bucket"], bucket_key))

    mock.add_children_payloads(key, children)
    mock.listen()

    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(mock.tasks[0].num_invokes, 3)
    self.assertTrue(mock.tasks[0].running)

if __name__ == "__main__":
  unittest.main()

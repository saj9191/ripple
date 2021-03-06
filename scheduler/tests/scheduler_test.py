import inspect
import json
import os
import sys
import time
import unittest
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import scheduler


class MockObject:
  def __init__(self, key):
    self.key = key


class MockLogger(scheduler.Logger):
  def __init__(self, queue):
    scheduler.Logger.__init__(self, queue)



class MockTask(scheduler.Task):
  def __init__(self, bucket_name, job, timeout, params, objects, queue, max_iterations, tokens):
    scheduler.Task.__init__(self, bucket_name, job, timeout, params, tokens)
    self.current_iteration = 0
    self.max_iterations = max_iterations
    self.invokes = []
    self.objects = objects
    self.queue = queue

  def __get_object__(self, prefix):
    keys = list(filter(lambda key: key.startswith(prefix), self.objects.keys()))
    assert(len(keys) == 1)
    return self.objects[keys[0]]

  def __get_objects__(self, stage):
    if stage not in self.queue:
      return []
    return self.queue[stage]

  def __invoke__(self, name, payload):
    self.invokes.append(payload)

  def __running__(self):
    running = self.current_iteration < self.max_iterations
    self.current_iteration += 1
    return running

  def __setup_client__(self):
    pass

  def __upload__(self):
    pass


class MockScheduler(scheduler.Scheduler):
  def __init__(self, policy, params, timeout, max_iterations):
    scheduler.Scheduler.__init__(self, policy, timeout, params)
    self.current_iteration = 0
    self.max_iterations = max_iterations
    self.objects = {}
    self.queue = {}

  def __add_task__(self, job):
    self.tasks.append(MockTask(self.params["log"], job, self.timeout, self.params, self.objects, self.queue, self.max_iterations, self.tokens))

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

  def add_job(self, queue, key):
    queue["messages"].append({"Body": json.dumps(scheduler.payload(self.params["bucket"], key))})
    self.queue[0] = [MockObject(key)]

  def add_to_queue(self, key):
    prefix = int(key.split("/")[0])
    if prefix not in self.queue:
      self.queue[prefix] = []
    self.queue[prefix].append(MockObject(key))

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
    mock.add_job(mock.log_queue, key)
    mock.listen()
    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(len(mock.tasks[0].invokes), 0)
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
    mock.add_job(mock.log_queue, key)
    children = []

    for i in range(1, 4):
      bucket_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      log_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.log".format(i)
      mock.add_to_queue(log_key)
      children.append(scheduler.payload(params["bucket"], bucket_key))

    mock.add_children_payloads(key, children)

    for i in range(1, 4):
      parent_log_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.log".format(i)
      children = []
      for j in range(1, 3):
        log_key = "2/123.400000-13/{1:d}-2/{0:d}-1-3-suffix.log".format(i, j)
        bucket_key = "2/123.400000-13/{1:d}-2/{0:d}-1-3-suffix.new".format(i, j)
        mock.add_to_queue(log_key)
        mock.add_children_payloads(log_key, [])
        children.append(scheduler.payload(params["bucket"], bucket_key))
      mock.add_children_payloads(parent_log_key, children)
    mock.listen()

    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(len(mock.tasks[0].invokes), 0)
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
    mock.add_job(mock.log_queue, key)
    children = []

    for i in range(1, 4):
      bucket_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      children.append(scheduler.payload(params["bucket"], bucket_key))

    mock.add_children_payloads(key, children)
    mock.listen()

    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(len(mock.tasks[0].invokes), 3)
    for i in range(3):
      self.assertEqual(mock.tasks[0].invokes[i]["Records"][0]["s3"]["object"]["key"], "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i + 1))
    self.assertTrue(mock.tasks[0].running)

  def test_pause(self):
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
    mock = MockScheduler("fifo", params, 0, 2)
    key = "0/123.400000-13/1-1/1-1-1-suffix.new"
    log = "0/123.400000-13/1-1/1-1-1-suffix.log"
    ctime = time.time()
    job = scheduler.Job("source_bucket", "destination_bucket", key, ctime - 2)
    job.pause = [ctime, job.start_time + 10]
    mock.add_jobs([job])
    time.sleep(2)
    children = []

    for i in range(1, 4):
      bucket_key = "1/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      children.append(scheduler.payload(params["bucket"], bucket_key))

    mock.add_children_payloads(log, children)
    mock.listen()
    self.assertEqual(len(mock.tasks[0].invokes), 0)


  def test_combine_retrigger(self):
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
    mock = MockScheduler("fifo", params, 0, 3)
    mock.add_job(mock.log_queue, key)

    # Stage 1. 3 bins, each with 2 files
    children = []
    for i in range(1, 4):
      for j in range(1, 3):
        bucket_key = "1/123.400000-13/{0:d}-3/{1:d}-1-2-suffix.new".format(i, j)
        child_key = "2/123.400000-13/{0:d}-3/{1:d}-1-2-suffix.new".format(i, j)
        log_key = "1/123.400000-13/{0:d}-3/{1:d}-1-2-suffix.log".format(i, j)
        mock.add_to_queue(log_key)
        children.append(scheduler.payload(params["bucket"], bucket_key))
        mock.add_children_payloads(log_key, [scheduler.payload(params["bucket"], child_key)])
    mock.add_children_payloads(key, children)

    # Stage 2. 1 bin, 3 files
    for i in range(1, 4):
      bucket_key = "2/123.400000-13/1-1/{0:d}-1-3-suffix.new".format(i)
      for j in range(1, 3):
        log_key = "2/123.400000-13/{0:d}-3/{1:d}-1-2-suffix.log".format(i, j)
        child_key = "1/123.400000-13/{0:d}-3/{1:d}-1-2-suffix.new".format(i, j)
        if i != 1 or j != 1:
          mock.add_to_queue(log_key)

    mock.listen()

    self.assertEqual(len(mock.tasks), 1)
    self.assertEqual(len(mock.tasks[0].invokes), 1)
    self.assertEqual(mock.tasks[0].invokes[0]["Records"][0]["s3"]["object"]["key"], "2/123.400000-13/1-3/1-1-2-suffix.new")
    self.assertTrue(mock.tasks[0].running)

class SimulationOrderTests(unittest.TestCase):
  def test_fifo(self):
    job1 = scheduler.Job("source_bucket", "destination_bucket", "key1", 10)
    job2 = scheduler.Job("source_bucket", "destination_bucket", "key2", 0)
    job3 = scheduler.Job("source_bucket", "destination_bucket", "key3", 20)

    orders = scheduler.simulation_order([job1, job2, job3], "fifo", 0.0, 3)
    expected = [job2, job1, job3]
    for i in range(len(expected)):
      self.assertEqual(orders[i][1], expected[i].start_time)
      self.assertEqual(orders[i][0], expected[i])

  def test_robin(self):
    job1 = scheduler.Job("source_bucket", "destination_bucket", "key1", 0)
    job2 = scheduler.Job("source_bucket", "destination_bucket", "key2", 0)
    job3 = scheduler.Job("source_bucket", "destination_bucket", "key3", 0)

    orders = scheduler.simulation_order([job1, job2, job3], "robin", 0.0, 3)
    expected = [job1, job2, job3]
    for i in range(len(expected)):
      self.assertEqual(orders[i][1], 2*i)
      self.assertEqual(orders[i][0], expected[i])

  def test_deadline(self):
    job1 = scheduler.Job("source_bucket", "destination_bucket", "key1", 0, None)
    job2 = scheduler.Job("source_bucket", "destination_bucket", "key2", 10, 40)
    job3 = scheduler.Job("source_bucket", "destination_bucket", "key3", 20, 60)

    orders = scheduler.simulation_order([job1, job2, job3], "deadline", 30.0, 2)
    expected = [job1, job2, job3]
    for i in range(len(expected)):
      self.assertEqual(orders[i][1], expected[i].start_time)
      self.assertEqual(orders[i][0], expected[i])

    self.assertEqual(orders[0][0].pause, [20.0, 40.0])

if __name__ == "__main__":
  unittest.main()

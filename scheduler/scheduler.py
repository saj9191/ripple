import argparse
import boto3
import inspect
import json
import os
import priority_queue
import random
import sys
import queue
import time
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util


class Scheduler:
  def __init__(self, policy, params):
    self.policy = policy
    self.params = params
    self.next_job_id = 0
    self.running_tasks = []
    self.max_tasks = 1000
    self.__setup__()

  def __setup__(self):
    if self.policy == "fifo":
      self.queue = priority_queue.Fifo()
    elif self.policy == "robin":
      self.queue = priority_queue.Robin()
    elif self.policy == "deadline":
      self.queue = priority_queue.Deadline()
    else:
      raise Exception("Unknown scheduling policy", self.policy)

    self.__aws_connections__()

  def __aws_connections__(self):
    self.s3 = boto3.resource("s3")
    self.client = util.setup_client("lambda", self.params)

  def add(self, priority, deadline, payload):
    item = priority_queue.Item(priority, 0, self.next_job_id, deadline, payload, self.params)
    self.queue.put(item)

  def __object_exists__(self, object_key):
    return util.object_exists(self.s3, self.params["log"], object_key)

  def __get_children_payloads__(self, object_key):
    obj = self.s3.Object(self.params["log"], object_key)
    content = obj.get()["Body"].read()
    params = json.loads(content.decode("utf-8"))
    return params["payloads"]

  def __check_tasks__(self):
    print("check_tasks")
    i = 0
    while i < len(self.running_tasks):
      task = self.running_tasks[i]
      if self.__object_exists__(task.output_file):
        payloads = self.__get_children_payloads__(task.output_file)
        for payload in payloads:
          self.add(task.priority, task.deadline, payload)
        self.running_tasks = self.running_tasks[:i] + self.running_tasks[i+1:]
      else:
        i += 1

  def __invoke__(self, name, payload):
    util.invoke(self.client, name, self.params, payload)

  def __payload__(self, bucket, key):
    return {
     "Records": [{
       "s3": {
         "bucket": {
           "name": bucket,
         },
         "object": {
           "key": key,
         },
       }
      }]
    }

  def __initiate_task__(self):
    try:
      item = self.queue.get()
      prefix = item.prefix
      name = self.params["pipeline"][prefix]["name"]
      item.payload["continue"] = True
      self.__invoke__(name, item.payload)
      self.running_tasks.append(item)
    except queue.Empty:
      time.sleep(random.randint(1, 10))

  def __running__(self):
    return self.running

  def listen(self):
    while self.__running__():
      if len(self.running_tasks) > 0:
        self.__check_tasks__()
      if len(self.running_tasks) < self.max_tasks:
        self.__initiate_task__()


def run(policy, params):
  scheduler = Scheduler(policy, params)
  scheduler.listen()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--policy", type=str, default="fifo", help="Scheduling policy to use (fifo, robin, deadline)")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(args.policy, params)

import boto3
import collections
import json
import node
import threading
import time
import ec2_util
from typing import Any, Dict, List, Optional, Tuple


class Run(threading.Thread):
  def __init__(self, master):
    super(Run, self).__init__()
    self.master = master

  def run(self):
    self.master.run()


class Master:
  error: Optional[str]
  num_tasks: int
  params: Dict[str, Any]
  pending_tasks: collections.deque
  results_folder: str
  running: bool
  scale_down_start_time: Optional[float]
  scale_up_start_time: Optional[float]
  starting_nodes: List[node.Node]
  running_nodes: List[node.Node]
  terminating_nodes: List[node.Node]
  total_node_count: int

  def __init__(self, s3_application_url: str, results_folder: str, params: Dict[str, Any]):
    self.error = None
    self.num_tasks = 0
    self.params = dict(params)
    self.pending_tasks = collections.deque()
    self.results_folder = results_folder
    self.running = True
    self.running_nodes = []
    self.starting_nodes = []
    self.s3_application_url = s3_application_url
    self.terminating_nodes = []
    self.scale_up_start_time = None
    self.scale_down_start_time = None
    self.total_node_count = 0

  def __check_for_new_items__(self):
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      AttributeNames=["SentTimestamp"],
      MaxNumberOfMessages=10,
      QueueUrl=self.queue.url,
      WaitTimeSeconds=self.params["s3_check_interval"],
    )
    messages = response["Messages"] if "Messages" in response else []
    for message in messages:
      body = json.loads(message["Body"])
      if "Records" in body:
        for record in body["Records"]:
          key: str = record["s3"]["object"]["key"]
          timestamp: float = float(message["Attributes"]["SentTimestamp"]) / 1000.0
          print("Adding task", key)
          self.pending_tasks.append(ec2_util.Task(key, timestamp))
      sqs.delete_message(QueueUrl=self.queue.url, ReceiptHandle=message["ReceiptHandle"])

  def __check_nodes__(self):
    print("Number of Pending Tasks", len(self.pending_tasks))
    i: int = 0
    while i < len(self.starting_nodes):
      n: node.Node = self.starting_nodes[i]
      n.reload()
      if n.state == "RUNNING":
        self.running_nodes.append(n)
        self.starting_nodes.pop(i)
      else:
        assert(n.state == "STARTING")
        i += 1

    self.__check_termination__()
    [cpu_average, num_tasks, num_tasks_average] = self.__compute_statistics__()
    self.num_tasks = num_tasks
    self.__scale_nodes__(cpu_average, num_tasks_average)

  def __check_termination__(self):
    i: int = 0
    while i < len(self.terminating_nodes):
      n: node.Node = self.terminating_nodes[i]
      if n.state == "TERMINATED":
        self.terminating_nodes.pop(i)
      else:
        assert(n.state == "TERMINATING")
        i += 1

  def __compute_statistics__(self) -> Tuple[float, int, float]:
    cpu_average: float = 0.0
    mem_average: float = 0.0
    num_tasks: int = 0
    num_tasks_average: float = 0.0

    nodes: List[node.Node] = self.running_nodes + self.terminating_nodes
    num_nodes: int = len(nodes)

    if len(nodes) > 0:
      for n in nodes:
        n.reload()
        if n.error is not None:
          self.error = n.error
        cpu_average += n.cpu_utilization
        mem_average += n.memory_utilization
        num_tasks += n.num_tasks
      cpu_average /= num_nodes
      mem_average /= num_nodes
      num_tasks_average = float(num_tasks) / num_nodes

    print("Average CPU Utilization", cpu_average)
    print("Average Memory Utilization", mem_average)
    print("Average Number of Tasks", num_tasks_average)
    print("Number of Running Nodes", len(self.running_nodes))
    print("Number of Starting Nodes", len(self.starting_nodes))
    print("Number of Terminating Nodes", len(self.terminating_nodes))
    print("")
    return (max(cpu_average, mem_average), num_tasks, num_tasks_average)

  def __create_node__(self):
    self.starting_nodes.append(node.Node(self.total_node_count, self.s3_application_url, self.pending_tasks, self.results_folder, self.params))
    self.total_node_count += 1

  def __scale_nodes__(self, cpu_average, num_tasks_average):
    if len(self.starting_nodes) == 0 and len(self.running_nodes) < self.params["max_nodes"]:
      if (len(self.starting_nodes + self.running_nodes) == 0 and len(self.pending_tasks) > 0):
        self.__create_node__()
        self.scale_up_start_time = time.time()
      elif cpu_average >= self.params["scale_up_utilization"]:
        if self.scale_up_start_time is None:
          self.scale_up_start_time = time.time()
        if time.time() - self.scale_up_start_time > self.params["scale_time"]:
          self.__create_node__()
      else:
        self.scale_up_start_time = None

    if len(self.terminating_nodes) == 0 and len(self.running_nodes) > 0:
      if cpu_average <= self.params["scale_down_utilization"]:
        if len(self.running_nodes) > 1 or len(self.running_nodes[0].tasks) == 0:
          self.running_nodes = sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
          if self.scale_down_start_time is None:
            self.scale_down_start_time = time.time()
          now = time.time()
          if now - self.scale_down_start_time > self.params["scale_time"]:
            if len(self.running_nodes) > 1 or len(self.pending_tasks) == 0:
              self.__terminate_node__()
              self.scale_down_start_time = time.time()
        else:
          self.scale_down_start_time = None
      else:
        self.scale_down_start_time = None

  def __setup_queue__(self):
    client = boto3.client("sqs", region_name=self.params["region"])
    response = client.list_queues(QueueNamePrefix=self.params["queue_name"])
    urls = response["QueueUrls"] if "QueueUrls" in response else []
    assert(len(urls) <= 1)
    sqs = boto3.resource("sqs")
    if len(urls) == 0:
      self.queue = sqs.create_queue(QueueName=self.queue_name, Attributes={"DelaySeconds": "5"})
    else:
      self.queue = sqs.get_queue_by_name(QueueName=self.params["queue_name"])
    # Remove stale SQS messages
    client.purge_queue(QueueUrl=self.queue.url)

  def __start_tasks__(self):
    if len(self.running_nodes) > 0:
      self.running_nodes = sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
      for n in self.running_nodes:
        n.add_tasks()

  def __terminate_node__(self):
    self.running_nodes = sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
    self.terminating_nodes.append(self.running_nodes.pop(0))
    self.terminating_nodes[-1].terminate()
    assert(self.terminating_nodes[-1].state == "TERMINATING")

  def __shutdown__(self):
    print("Shutting down...")
    for n in self.running_nodes + self.starting_nodes:
      self.terminating_nodes.append(n)
      n.terminate()
      assert(n.state == "TERMINATING")

    self.running_nodes = []
    self.starting_nodes = []
    while len(self.terminating_nodes) > 0:
      print("Shutting down!", self.running)
      self.__check_nodes__()
      time.sleep(10)

  def run(self):
    nodes = self.starting_nodes + self.running_nodes + self.terminating_nodes
    while len(self.pending_tasks) > 0 or self.num_tasks > 0 or len(nodes) > 0 or self.running:
      self.__check_for_new_items__()
      self.__check_nodes__()
      self.__start_tasks__()
      time.sleep(self.params["s3_check_interval"])

    self.__shutdown__()

  def setup(self):
    self.__create_node__()
    self.__setup_queue__()

  def shutdown(self):
    self.running = False

  def start(self, asynch: bool):
    if asynch:
      r = Run(self)
      r.start()
    else:
      self.run()

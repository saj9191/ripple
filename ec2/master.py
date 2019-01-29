import boto3
import collections
import json
import node
import threading
import time


class Task:
  def __init__(self, key, timestamp):
    self.key = key
    self.created_at = timestamp
    self.received_at = time.time()


class Run(threading.Thread):
  def __init__(self, master):
    super(Run, self).__init__()
    self.master = master

  def run(self):
    self.master.run()


class Master:
  def __init__(self, s3_application_url, results_folder, params):
    self.error = None
    self.num_tasks_average = 0.0
    self.params = dict(params)
    self.pending_tasks = collections.deque()
    self.results_folder = results_folder
    self.running = True
    self.running_nodes = []
    self.starting_nodes = []
    self.s3_application_url = s3_application_url
    self.terminating_nodes = []
    self.termination_count = 0
    self.total_node_count = 0

  def __check_for_new_items__(self):
    if len(self.running_nodes) == 0:
      return
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
          key = record["s3"]["object"]["key"]
          timestamp = float(message["Attributes"]["SentTimestamp"]) / 1000.0
          print("Adding task", key)
          self.pending_tasks.append(Task(key, timestamp))
      sqs.delete_message(QueueUrl=self.queue.url, ReceiptHandle=message["ReceiptHandle"])

  def __check_nodes__(self):
    print("Number of Pending Tasks", len(self.pending_tasks))
    i = 0
    while i < len(self.starting_nodes):
      n = self.starting_nodes[i]
      n.reload()
      if n.state == "RUNNING":
        self.running_nodes.append(n)
        self.starting_nodes = self.starting_nodes[:i] + self.starting_nodes[i+1:]
      else:
        assert(n.state == "STARTING")
        i += 1

    self.__check_termination__()
    [cpu_average, num_tasks_average] = self.__compute_statistics__()
    self.num_tasks_average = num_tasks_average
    self.__scale_nodes__(cpu_average, num_tasks_average)

  def __check_termination__(self):
    i = 0
    while i < len(self.terminating_nodes):
      n = self.terminating_nodes[i]
      if n.state == "TERMINATED":
        self.terminating_nodes = self.terminating_nodes[:i] + self.terminating_nodes[i+1:]
      else:
        assert(n.state == "TERMINATING")
        i += 1

  def __compute_statistics__(self):
    cpu_average = 0.0
    mem_average = 0.0
    num_tasks_average = 0.0

    nodes = self.running_nodes + self.terminating_nodes
    num_nodes = len(nodes)

    if len(nodes) > 0:
      for n in nodes:
        n.reload()
        if n.error is not None:
          self.error = n.error
        cpu_average += n.cpu_utilization
        mem_average += n.memory_utilization
        num_tasks_average += n.num_tasks
      cpu_average /= num_nodes
      mem_average /= num_nodes
      num_tasks_average /= num_nodes

    print("Average CPU Utilization", cpu_average)
    print("Average Memory Utilization", mem_average)
    print("Average Number of Tasks", num_tasks_average)
    print("Number of Running Nodes", len(self.running_nodes))
    print("Number of Starting Nodes", len(self.starting_nodes))
    print("Number of Terminating Nodes", len(self.terminating_nodes))
    print("")
    return [cpu_average, num_tasks_average]

  def __create_node__(self):
    self.starting_nodes.append(node.Node(self.total_node_count, self.s3_application_url, self.pending_tasks, self.results_folder, self.params))
    self.total_node_count += 1

  def __scale_nodes__(self, cpu_average, num_tasks_average):
    if len(self.starting_nodes) == 0 and len(self.running_nodes) < self.params["max_nodes"]:
      if cpu_average >= self.params["scale_up_utilization"]:
        self.__create_node__()
      elif len(self.pending_tasks) > 0 and len(self.running_nodes) == 0:
        self.__create_node__()

    if len(self.terminating_nodes) == 0:
      if len(self.running_nodes) > 1 or (len(self.pending_tasks) == 0 and len(self.running_nodes) > 0):
        if cpu_average <= self.params["scale_down_utilization"]:
          self.running_nodes = sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
          if len(self.running_nodes) > 1:  # or self.running_nodes[0].num_tasks == 0:
            self.termination_count += 1
            if self.termination_count == self.params["termination_count"]:
              self.__terminate_node__()
              self.termination_count = 0
        else:
          self.termination_count = 0

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
      self.__check_nodes__()
      time.sleep(10)

  def run(self):
    while len(self.pending_tasks) > 0 or self.num_tasks_average > 0.0 or self.running:
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

  def start(self, asynch):
    if asynch:
      r = Run(self)
      r.start()
    else:
      self.run()

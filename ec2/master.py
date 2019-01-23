import boto3
import json
import node
import threading
import time


class Run(threading.Thread):
  def __init__(self, master):
    super(Run, self).__init__()
    self.master = master

  def run(self):
    self.master.run()


class Master:
  def __init__(self, bucket_name, max_nodes, s3_application_url, params):
    self.bucket_name = bucket_name
    self.max_nodes = max_nodes
    self.params = dict(params)
    self.pending_tasks = []
    self.queue_name = "shjoyner-sqs"
    self.running = True
    self.running_nodes = []
    self.starting_nodes = []
    self.s3_application_url = s3_application_url
    self.terminating_nodes = []
    self.num_handled_tasks = 0

  def __check_for_new_items__(self):
    if len(self.running_nodes) == 0:
      return
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      QueueUrl=self.queue.url,
      WaitTimeSeconds=self.params["s3_check_interval"],
    )
    messages = response["Messages"] if "Messages" in response else []
    for message in messages:
      body = json.loads(message["Body"])
      if "Records" in body:
        for record in body["Records"]:
          print("Received item", record["s3"]["object"]["key"])
          self.pending_tasks.append(record["s3"]["object"]["key"])
      sqs.delete_message(QueueUrl=self.queue.url, ReceiptHandle=message["ReceiptHandle"])

  def __check_nodes__(self):
    i = 0
    while i < len(self.starting_nodes):
      n = self.starting_nodes[i]
      n.reload()
      if n.node is not None:
        print("Reloaded", n.node.instance_id, n.cpu_utilization)
      if n.state == "RUNNING":
        self.running_nodes.append(n)
        self.starting_nodes = self.starting_nodes[:i] + self.starting_nodes[i+1:]
      else:
        assert(n.state == "STARTING")
        i += 1

    self.__check_termination__()
    [cpu_average, num_tasks_average] = self.__compute_statistics__()
    self.__scale_nodes__(cpu_average, num_tasks_average)

  def __check_termination__(self):
    i = 0
    while i < len(self.terminating_nodes):
      n = self.terminating_nodes[i]
      n.reload()
      if n.state == "TERMINATED":
        self.terminating_nodes = self.terminating_nodes[:i] + self.terminating_nodes[i+1:]
      else:
        assert(n.state == "TERMINATING")
        i += 1

  def __compute_statistics__(self):
    cpu_average = 0.0
    num_tasks_average = 0.0
    memory_average = 0.0

    if len(self.running_nodes) > 0:
      for n in self.running_nodes:
        n.reload()
        cpu_average += n.cpu_utilization
        memory_average += n.memory_utilization
        num_tasks_average += n.num_tasks
      cpu_average /= len(self.running_nodes)
      memory_average /= len(self.running_nodes)
      num_tasks_average /= len(self.running_nodes)

    assert(0.0 <= cpu_average and cpu_average <= 100.0)
    assert(0.0 <= num_tasks_average and num_tasks_average <= self.params["max_tasks_per_node"])
    print("Average CPU utilization", cpu_average)
    print("Average Memory utilization", memory_average)
    print("Average Number of tasks", num_tasks_average)
    print("Number of running nodes", len(self.running_nodes))
    print("Number of starting nodes", len(self.starting_nodes))
    print("")
    return [cpu_average, num_tasks_average]

  def __create_node__(self):
    self.starting_nodes.append(node.Node(self.s3_application_url, self.params))

  def __scale_nodes__(self, cpu_average, num_tasks_average):
    if len(self.starting_nodes) == 0:
      if cpu_average >= self.params["scale_up_utilization"] or num_tasks_average == self.params["max_tasks_per_node"]:
        self.__create_node__()
      elif len(self.pending_tasks) > 0 and len(self.running_nodes) == 0:
        self.__create_node__()

    if len(self.terminating_nodes) == 0:
      if len(self.running_nodes) > 1 or len(self.pending_tasks) == 0:
        if cpu_average <= self.params["scale_down_utilization"]:
          self.__terminate_node__()

  def __setup_queue__(self):
    client = boto3.client("sqs", region_name=self.params["region"])
    response = client.list_queues(QueueNamePrefix=self.queue_name)
    urls = response["QueueUrls"] if "QueueUrls" in response else []
    assert(len(urls) <= 1)
    sqs = boto3.resource("sqs")
    if len(urls) == 0:
      self.queue = sqs.create_queue(QueueName=self.queue_name, Attributes={"DelaySeconds": "5"})
    else:
      self.queue = sqs.get_queue_by_name(QueueName=self.queue_name)

  def __start_tasks__(self):
    if len(self.running_nodes) > 0:
      sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
      for n in self.running_nodes:
        print("Instance", n.node.instance_id, "has", n.num_tasks, "tasks")
        if n.num_tasks < self.params["max_tasks_per_node"] and len(self.pending_tasks) > 0:
          if n.cpu_utilization <= self.params["scale_up_utilization"]:
            n.add_task(self.pending_tasks.pop())
            self.num_handled_tasks += 1

  def __terminate_node__(self):
    sorted(self.running_nodes, key=lambda n: n.cpu_utilization)
    self.terminating_nodes.append(self.running_nodes.pop())
    self.terminating_nodes[-1].terminate()
    assert(self.terminating_nodes[-1].state == "TERMINATING")
    print("Num terminated", len(self.terminating_nodes))
    print("Num running", len(self.running_nodes))

  def __shutdown__(self):
    print("Shutting down...")
    for n in self.running_nodes + self.starting_nodes:
      self.terminating_nodes.append(n)
      n.terminate()
      assert(n.state == "TERMINATING")

    self.running_nodes = []
    self.starting_nodes = []
    while len(self.terminating_nodes) > 0:
      self.__check_termination__()
      time.sleep(10)

    print("Handled", self.num_handled_tasks, "tasks")

  def run(self):
    while self.running:
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
      Run(self)
    else:
      self.run()

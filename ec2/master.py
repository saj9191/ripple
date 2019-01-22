import boto3
import json
import node
import random
import time


class Task:
  def __init__(self, s3_location):
    self.s3_location = s3_location


class Master:
  def __init__(self, bucket_name, max_nodes, s3_application_url, params):
    self.bucket_name = bucket_name
    self.max_nodes = max_nodes
    self.params = dict(params)
    self.pending_tasks = []
    self.queue_name = "shjoyner-sqs"
    self.running_nodes = []
    self.starting_nodes = []
    self.s3_application_url = s3_application_url
    self.terminating_nodes = []

  def __check_for_new_items__(self):
    if len(self.running_nodes) == 0:
      return
    x = random.randint(1, 10)
    print("Adding", x, "items")
    for i in range(x):
      self.pending_tasks.append("20140329_Yeast_DDA_60mins_01.mzML")
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
    cpu_average = 0.0
    memory_average = 0.0
    num_tasks_average = 0.0
    i = 0
    while i < len(self.starting_nodes):
      n = self.starting_nodes[i]
      n.reload()
      if n.state == "RUNNING":
        self.running_nodes.append(n)
        self.starting_nodes = self.starting_nodes[:i] + self.starting_nodes[i+1:]
      else:
        i += 1

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
    print("Average CPU utilization", cpu_average)
    print("Average Memory utilization", memory_average)
    print("Average Number of tasks", num_tasks_average)
    print("Number of running nodes", len(self.running_nodes))
    print("Number of starting nodes", len(self.starting_nodes))
    print("")
    if len(self.starting_nodes) == 0:
      if cpu_average >= self.params["scale_up_utilization"] or num_tasks_average == self.params["max_tasks_per_node"]:
        self.__create_node__()

  def __create_node__(self):
    self.starting_nodes.append(node.Node(self.s3_application_url, self.params))

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

  def setup(self):
    self.__create_node__()
    self.__setup_queue__()

  def shutdown(self):
    print("Shutting down...")
    for n in self.running_nodes + self.starting_nodes:
      n.terminate()

  def run(self):
    for i in range(60):
      self.__check_for_new_items__()
      self.__check_nodes__()
      self.__start_tasks__()
      time.sleep(10)

    self.shutdown()

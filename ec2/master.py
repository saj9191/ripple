import boto3
import node
import time
import util


class Task:
  def __init__(self, s3_location):
    self.s3_location = s3_location


class Master:
  def __init__(self, bucket_name, max_nodes, params):
    self.bucket_name = bucket_name
    self.max_nodes = max_nodes
    self.starting_nodes = []
    self.running_nodes = []
    self.terminating_nodes = []
    self.params = dict(params)
    self.pending_tasks = []
    self.queue_name = "shjoyner-sqs"

  def __check_for_new_items__(self):
    sqs = boto3.client("sqs", region_name=self.params["region"])
    response = sqs.receive_message(
      QueueUrl=self.queue.url,
      WaitTimeSeconds=self.params["s3_check_interval"],
    )
    messages = response["Messages"] if "Messages" in response else []
    print(time.time(), "Received", len(messages), "messages")
    for message in messages:
      print(message)

  def __check_nodes__(self):
    cpu_average = 0.0
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
      cpu_average /= len(self.running_nodes)

    assert(0.0 <= cpu_average and cpu_average <= 100.0)
    print("Average CPU utilization", cpu_average)
    if cpu_average >= self.params["scale_up_cpu_utilization"]:
      self.__create_node__()

  def __create_node__(self):
    self.starting_nodes.append(node.Node(self.params))

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
    pass

  def setup(self):
    self.master_instance = util.create_instance("emr-master-{0:f}".format(time.time()), self.params)
    self.__create_node__()
    self.__setup_queue__()

  def shutdown(self):
    print("Shutting down...")
    for n in self.running_nodes + self.starting_nodes:
      n.terminate()

    self.master_instance.terminate()
    self.master_instance.wait_until_terminated()

  def run(self):
    for i in range(10):
      self.__check_for_new_items__()
      self.__check_nodes__()
      self.__start_tasks__()
      time.sleep(30)

    self.shutdown()

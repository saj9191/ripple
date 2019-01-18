import boto3
import datetime
import time


class Task:
  def __init__(self, s3_location):
    self.s3_location = s3_location


class Master:
  def __init__(self, bucket_name, max_nodes, params):
    self.bucket_name = bucket_name
    self.max_nodes = max_nodes
    self.nodes = []
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

  def __check_nodes_health__(self):
    now = datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
    cpu_average = 0.0
    for node in self.nodes:
      cpu_average += self.__node_statistics__(node, now)

    cpu_average /= len(self.nodes)
    assert(0.0 <= cpu_average and cpu_average <= 100.0)
    print("Average CPU utilization", cpu_average)
    if cpu_average >= self.params["scale_up_cpu_utilization"]:
      self.nodes.append(self.__create_instance__("emr-node-{0:f}".format(time.time()), monitor=True))

  def __create_instance__(self, tag_name, monitor):
    print("Creating instance", tag_name)
    ec2 = boto3.resource("ec2")
    instances = ec2.create_instances(
      ImageId=self.params["image_id"],
      InstanceType=self.params["instance"],
      KeyName=self.params["key"],
      MinCount=1,
      MaxCount=1,
      NetworkInterfaces=[{
        "SubnetId": self.params["subnet"],
        "DeviceIndex": 0,
        "Groups": [self.params["security"]]
      }],
      TagSpecifications=[{
        "ResourceType": "instance",
        "Tags": [{
          "Key": "Name",
          "Value": tag_name,
        }]
      }]
    )
    assert(len(instances) == 1)
    instance = instances[0]
    if monitor:
      client = boto3.client("ec2")
      client.monitor_instances(InstanceIds=[instance.instance_id])

    instance.wait_until_running()
    return instance

  def __node_statistics__(self, node, now):
    client = boto3.client("cloudwatch", region_name=self.params["region"])

    response = client.get_metric_statistics(
      Namespace="AWS/EC2",
      MetricName="CPUUtilization",
      Dimensions=[{
        "Name": "InstanceId",
        "Value": node.instance_id,
      }],
      StartTime=now - datetime.timedelta(seconds=self.params["cpu_agg_period"] + 120),  # Additional time in case times rounded
                                                                                        # if EndTime - StartTime < agg_period, may not return anything
      EndTime=now,
      Period=self.params["cpu_agg_period"],
      Statistics=["Average"],
    )
    datapoints = response["Datapoints"]

    point = datapoints[-1]["Average"] if len(datapoints) > 0 else 0.0
    return point

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
    self.master_instance = self.__create_instance__("emr-master-{0:f}".format(time.time()), monitor=False)
    self.nodes.append(self.__create_instance__("emr-node-{0:f}".format(time.time()), monitor=True))
    self.__setup_queue__()

  def shutdown(self):
    print("Shutting down...")
    for node in self.nodes:
      node.terminate()

    self.master_instance.terminate()
    self.master_instance.wait_until_terminated()

  def run(self):
    for i in range(10):
      self.__check_for_new_items__()
      self.__check_nodes_health__()
      self.__start_tasks__()
      time.sleep(30)

    self.shutdown()

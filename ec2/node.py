import boto3
import datetime
import paramiko
import threading
import time
import util


class Task(threading.Thread):
  def __init__(self, node_ip, key):
    self.node_ip = node_ip
    self.__setup__(key + ".pem")

  def __setup__(self, pem):
    self.ssh_client = paramiko.SSHClient()
    key = paramiko.RSAKey.from_private_key_file(pem)
    self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    self.ssh_client.connect(self.node_ip, username="ubuntu", pkey=key)

  def run(self):
    pass


class Node:
  def __init__(self, params):
    self.cpu_utilization = 0.0
    self.params = params
    self.state = "STARTING"
    self.tasks = []
    self.__setup__()

  def __setup__(self):
    self.node = util.create_instance("emr-node-{0:f}".format(time.time()), self.params)
    boto3.client("ec2").monitor_instances(InstanceIds=[self.node.instance_id])

  def reload(self):
    now = datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
    client = boto3.client("cloudwatch", region_name=self.params["region"])

    response = client.get_metric_statistics(
      Namespace="AWS/EC2",
      MetricName="CPUUtilization",
      Dimensions=[{
        "Name": "InstanceId",
        "Value": self.node.instance_id,
      }],
      StartTime=now - datetime.timedelta(seconds=self.params["cpu_agg_period"] + 120),  # Additional time in case times rounded
                                                                                        # if EndTime - StartTime < agg_period, may not return anything
      EndTime=now,
      Period=self.params["cpu_agg_period"],
      Statistics=["Average"],
    )
    datapoints = response["Datapoints"]
    assert(self.state == "STARTING" or len(datapoints) > 0)
    if len(datapoints) > 0:
      self.state = "RUNNING"

    self.cpu_utilization = datapoints[-1]["Average"] if len(datapoints) > 0 else 0.0

  def add_task(self):
    self.tasks.append(Task(self.node.public_ip_address, self.params["key"]))
    self.tasks[-1].start()

  def terminate(self):
    self.node.terminate()

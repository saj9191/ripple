import boto3
import datetime
import util
import time


class Node:
  def __init__(self, params):
    self.params = params
    self.state = "STARTING"
    self.cpu_utilization = 0.0
    self.__setup__()

  def __setup__(self):
    self.node = util.create_instance("emr-node-{0:f}".format(time.time()), self.params)
    client = boto3.client("ec2")
    client.monitor_instances(InstanceIds=[self.node.instance_id])

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

  def terminate(self):
    self.node.terminate()

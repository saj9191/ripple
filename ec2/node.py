import boto3
import datetime
import os
import paramiko
import threading
import time
import util


def create_client(node_ip, pem):
  ssh_client = paramiko.SSHClient()
  key = paramiko.RSAKey.from_private_key_file(pem)
  ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  connected = False
  while not connected:
    try:
      ssh_client.connect(node_ip, username="ubuntu", pkey=key)
      connected = True
    except paramiko.ssh_exception.NoValidConnectionsError:
      time.sleep(1)
  return ssh_client


def exec_command(ssh_client, command):
  (stdin, stdout, stderr) = ssh_client.exec_command(command)
  stdout.channel.recv_exit_status()
  output = stdout.read().decode("utf-8")
  err = stderr.read().decode("utf-8")
  return [output, err]


def get_credentials():
  home = os.path.expanduser("~")
  f = open("{0:s}/.aws/credentials".format(home))
  lines = f.readlines()
  header = "[default]"
  for i in range(len(lines)):
    if lines[i].strip() == header:
      access_key = lines[i + 1].split("=")[1].strip()
      secret_key = lines[i + 2].split("=")[1].strip()
      return [access_key, secret_key]


class Task(threading.Thread):
  def __init__(self, node_ip, key, folder, file):
    super(Task, self).__init__()
    print("Thread handling file", file)
    self.file = file
    self.folder = folder
    self.node_ip = node_ip
    self.__setup__(key + ".pem")
    self.running = True

  def __setup__(self, pem):
    self.client = create_client(self.node_ip, pem)

  def run(self):
    start_time = time.time()
    _, err = exec_command(self.client, "cd ~/{0:s}; python3 main.py {1:s}".format(self.folder, self.file))
    end_time = time.time()
    open("simulations/round1/{0:f}-{1:f}".format(start_time, end_time), "a+")
    self.client.close()
    self.running = False


class Setup(threading.Thread):
  def __init__(self, node):
    super(Setup, self).__init__()
    self.node = node

  def run(self):
    node = util.create_instance("emr-node-{0:f}".format(time.time()), self.node.params)
    node.reload()
    boto3.client("ec2").monitor_instances(InstanceIds=[node.instance_id])
    client = create_client(node.public_ip_address, self.node.params["key"] + ".pem")
    [access, secret] = get_credentials()
    exec_command(client, "mkdir ~/.aws")
    exec_command(client, "touch ~/.aws/credentials")
    # The monitor script creates a file that tracks the instance id. I didn't delete this before I created the AMI, so if this
    # is not deleted, it will always use the same instance id.
    exec_command(client, "rm /var/tmp/aws-mon/instance-id")
    exec_command(client, 'echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/.aws/credentials'.format(access, secret))
    exec_command(client, 'echo -e "{0:s}\n{1:s}\n\n\n\n\n\nY\ny\n" | s3cmd --configure'.format(access, secret))
    exec_command(client, 'echo "AWSAccessKeyId={0:s}\nAWSSecretKey={1:s}" >> aws-scripts-mon/awscreds.conf'.format(access, secret))
    c = '(crontab -l 2>/dev/null; echo "* * * * * ~/aws-scripts-mon/mon-put-instance-data.pl --mem-used-incl-cache-buff --mem-util --disk-space-util --disk-path=/ --from-cron") | crontab -'
    exec_command(client, c)
    _, err = exec_command(client, "s3cmd get {0:s} --recursive".format(self.node.s3_application_url))
    client.close()
    self.node.node = node


class Node:
  def __init__(self, s3_application_url, params):
    self.cpu_utilization = 0.0
    self.folder = s3_application_url.split("/")[-1]
    self.node = None
    self.num_tasks = 0
    self.params = params
    self.s3_application_url = s3_application_url
    self.state = "STARTING"
    self.tasks = []
    self.__setup__()

  def __setup__(self):
    self.setup = Setup(self)
    self.setup.start()

  def add_task(self, file):
    print("Node", self.node.instance_id, "handling task", file)
    self.tasks.append(Task(self.node.public_ip_address, self.params["key"], self.folder, file))
    self.tasks[-1].start()
    self.num_tasks = len(self.tasks)
    print("Node", self.node.instance_id, "has ", len(self.tasks), "tasks")

  def __get_metric__(self, client, namespace, metric, start_time, end_time, period):
    response = client.get_metric_statistics(
      Namespace=namespace,
      MetricName=metric,
      Dimensions=[{
        "Name": "InstanceId",
        "Value": self.node.instance_id,
      }],
      StartTime=start_time,
      EndTime=end_time,
      Period=period,
      Statistics=["Average"],
    )
    datapoints = response["Datapoints"]
    return datapoints

  def reload(self):
    if self.node is None:
      return
    elif self.setup is not None:
      self.setup.join()
      self.setup = None

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running:
        self.tasks[i].join()
        self.tasks = self.tasks[:i] + self.tasks[i + 1:]
      else:
        i += 1
    self.num_tasks = len(self.tasks)

    if self.state in ["TERMINATING", "TERMINATED"]:
      if self.state == "TERMINATING" and self.num_tasks == 0:
        self.node.terminate()
        self.state = "TERMINATED"
      return

    end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
    # Additional time in case time is rounded. If end_time - start_time < agg_period, may not return anything
    start_time = end_time - datetime.timedelta(seconds=self.params["agg_period"] + 5*600)
    client = boto3.client("cloudwatch", region_name=self.params["region"])
    cpu_datapoints = self.__get_metric__(client, "AWS/EC2", "CPUUtilization", start_time, end_time, self.params["agg_period"])
    memory_datapoints = self.__get_metric__(client, "System/Linux", "MemoryUtilization", start_time, end_time, 60)
    assert(self.state == "STARTING" or len(cpu_datapoints) > 0)
    if len(memory_datapoints) > 0 and len(cpu_datapoints) > 0:
      self.state = "RUNNING"

    self.cpu_utilization = cpu_datapoints[-1]["Average"] if len(cpu_datapoints) > 0 else 0.0
    self.memory_utilization = memory_datapoints[-1]["Average"] if len(memory_datapoints) > 0 else 0.0

  def terminate(self):
    self.state = "TERMINATING"

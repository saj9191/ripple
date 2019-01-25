import boto3
import os
import paramiko
import random
import re
import threading
import time

STATS_REGEX = re.compile("([0-9\.]+)%\s+([0-9\.]+)%")


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
    except paramiko.ssh_exception.SSHException:
      time.sleep(1)
  return ssh_client


def create_instance(tag_name, params):
  print("Creating instance", tag_name)
  ec2 = boto3.resource("ec2")
  instances = ec2.create_instances(
    BlockDeviceMappings=[{
      "DeviceName": "/dev/xvda",
      "Ebs": {
        "VolumeSize": params["volume_size"]
      }
    }],
    ImageId=params["image_id"],
    InstanceType=params["instance"],
    KeyName=params["key"],
    MinCount=1,
    MaxCount=1,
    NetworkInterfaces=[{
      "SubnetId": params["subnet"],
      "DeviceIndex": 0,
      "Groups": [params["security"]]
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
  instance.wait_until_running()
  return instance


def exec_command(ssh_client, command):
  done = False
  while not done:
    try:
      (stdin, stdout, stderr) = ssh_client.exec_command(command)
      done = True
    except TimeoutError as e:
      pass
  code = stdout.channel.recv_exit_status()
  output = stdout.read().decode("utf-8")
  err = stderr.read().decode("utf-8")
  return [code, output, err]


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
  def __init__(self, results_folder, node_ip, key, folder, pending_queue):
    super(Task, self).__init__()
    self.cpu = 170
    self.error = None
    self.file = pending_queue.get()
    self.folder = folder
    self.memory = 2*1024*1024*1024
    self.node_ip = node_ip
    self.pending_queue = pending_queue
    self.results_folder = results_folder
    self.running = True
    self.__setup__(key + ".pem")

  def __setup__(self, pem):
    self.client = create_client(self.node_ip, pem)

  def run(self):
    start_time = time.time()
    name = random.randint(1, 100*1000)
    c = "sudo docker run --name {0:d} -m {1:d} --cpu-shares {2:d} app python3 main.py {3:s}".format(name, self.memory, self.cpu, self.file.key)
    code, output, err = exec_command(self.client, c)
    self.code = code
    end_time = time.time()
    if code != 0:
      if code != 125:
        print("CODE:", code)
        print("OUTPUT:", output)
        print("ERROR:", err)
        self.error = err
      self.pending_queue.put(self.file)
    else:
      with open("{0:s}/tasks/{1:f}-{2:f}".format(self.results_folder, start_time, end_time), "w+") as f:
        f.write("S3 CREATED TIME: {0:f}\n".format(self.file.created_at))
        f.write("EXECUTION START TIME: {0:f}\n".format(start_time))
        f.write("EXECUTION END TIME: {0:f}\n".format(end_time))
        f.write("KEY NAME: {0:s}\n".format(self.file.key))
    code, _, _ = exec_command(self.client, "sudo docker rm {0:d}".format(name))
    assert(code == 0)
    self.client.close()
    self.running = False


class Setup(threading.Thread):
  def __init__(self, node):
    super(Setup, self).__init__()
    self.node = node

  def run(self):
    node = create_instance("emr-node-{0:f}".format(time.time()), self.node.params)
    node.reload()
#    boto3.client("ec2").monitor_instances(InstanceIds=[node.instance_id])
    client = create_client(node.public_ip_address, self.node.params["key"] + ".pem")
    [access, secret] = get_credentials()
    exec_command(client, 'echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/.aws/credentials'.format(access, secret))
    exec_command(client, 'echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/Docker/app/credentials'.format(access, secret))
    exec_command(client, 'echo -e "{0:s}\n{1:s}\n\n\n\n\n\nY\ny\n" | s3cmd --configure'.format(access, secret))
    code, stdout, err = exec_command(client, "cd ~/Docker/app; s3cmd get {0:s}/ . --recursive --force".format(self.node.s3_application_url))
    code, _, _ = exec_command(client, "cd ~/Docker; sudo docker build --tag=app .")
    assert(code == 0)
    _, stdout, _ = exec_command(client, "grep -c ^processor /proc/cpuinfo")
    self.node.num_cpus = int(stdout.strip())
    client.close()
    self.node.node = node


class Node:
  def __init__(self, node_id, s3_application_url, pending_queue, results_folder, params):
    self.cpu_utilization = 0.0
    self.error = None
    self.folder = s3_application_url.split("/")[-1]
    self.memory_utilization = 0.0
    self.num_cpus = 0
    self.node = None
    self.node_id = node_id
    self.num_tasks = 0
    self.params = params
    self.pending_queue = pending_queue
    self.results_folder = results_folder
    self.create_time = time.time()
    self.s3_application_url = s3_application_url
    self.state = "STARTING"
    self.tasks = []
    self.__setup__()

  def __setup__(self):
    self.setup = Setup(self)
    self.setup.start()

  def add_tasks(self):
    while self.pending_queue.qsize() > 0:
      print(self.node.instance_id, "Adding task")
      self.tasks.append(Task(self.results_folder, self.node.public_ip_address, self.params["key"], self.folder, self.pending_queue))
      self.tasks[-1].start()
    self.num_tasks = len(self.tasks)

  def __get_metrics__(self):
    metrics = [0.0, 0.0]
    code, output, err = exec_command(self.client, 'sudo docker stats --format "table {{.CPUPerc}}\t{{.MemPerc}}" --no-stream')
    lines = list(filter(lambda line: len(line) > 0, output.split("\n")[1:]))
    for line in lines:
      m = STATS_REGEX.search(line)
      assert(m is not None)
      for i in range(len(metrics)):
        metrics[i] += float(m.group(i + 1))

    metrics[0] /= self.num_cpus
    return metrics

  def __terminate__(self):
    self.node.terminate()
    self.state = "TERMINATED"
    end_time = time.time()
    with open("{0:s}/nodes/{1:d}".format(self.results_folder, self.node_id), "w+") as f:
      f.write("NODE CREATE TIME: {0:f}\n".format(self.create_time))
      f.write("NODE START TIME: {0:f}\n".format(self.start_time))
      f.write("NODE TERMINATE TIME: {0:f}\n".format(self.terminate_time))
      f.write("NODE END TIME: {0:f}\n".format(end_time))

  def reload(self):
    if self.node is None:
      return
    elif self.setup is not None:
      self.setup.join()
      self.setup = None
      self.client = create_client(self.node.public_ip_address, self.params["key"] + ".pem")
      self.state = "RUNNING"
      self.start_time = time.time()

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running:
        if self.tasks[i].error is not None:
          self.error = self.tasks[i].error
        self.tasks[i].join()
        self.tasks = self.tasks[:i] + self.tasks[i + 1:]
      else:
        i += 1

    self.num_tasks = len(self.tasks)
    if self.state in ["TERMINATING", "TERMINATED"]:
      if self.state == "TERMINATING" and self.num_tasks == 0:
        self.__terminate__()
      return

    [cpu, mem] = self.__get_metrics__()
    self.cpu_utilization = cpu
    self.memory_utilization = mem
    print(self.node.instance_id, "CPU Utilization", self.cpu_utilization, "Memory", self.memory_utilization)

  def terminate(self):
    self.terminate_time = time.time()
    self.state = "TERMINATING"

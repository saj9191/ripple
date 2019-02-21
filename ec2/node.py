import boto3
import collections
import os
import paramiko
import random
import re
import threading
import time
import ec2_util
from enum import Enum
from typing import Any, Dict, List, Optional, Pattern, Tuple

STATS_REGEX = re.compile("([0-9\.]+)%\s+([0-9\.]+)%")


class Client:
  client: paramiko.SSHClient
  ip: str
  pem: str
  timeout: int

  def __init__(self, ip: str, pem: str, timeout: int):
    self.ip = ip
    self.pem = pem
    self.timeout = timeout
    self.__create__()

  def __create__(self):
    done: bool = False
    i: int = 0
    while not done and i < 3:
      try:
        self.client = self.__create_client__(self.ip, self.pem)
        done = True
      except TimeoutError as e:
        i += 1
        time.sleep(1)
    if not done:
      raise Exception("Cannot connect to node", self.ip)

  def __create_client__(self, node_ip: str, pem: str):
    ssh_client = paramiko.SSHClient()
    key = paramiko.RSAKey.from_private_key_file(pem)
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connected: bool = False
    while not connected:
      try:
        ssh_client.connect(node_ip, username="ubuntu", pkey=key)
        connected = True
      except paramiko.ssh_exception.NoValidConnectionsError:
        time.sleep(1)
      except paramiko.ssh_exception.SSHException:
        time.sleep(1)
    return ssh_client

  def exec_command(self, command: str) -> Tuple[int, str, str]:
    done = False
    i = 0
    while not done and i < 3:
      try:
        (stdin, stdout, stderr) = self.client.exec_command(command, timeout=self.timeout)
        done = True
      except Exception as e:
        i += 1
        self.__create__()
        time.sleep(1)
    if not done:
      raise Exception("Cannot execute command", command)
    code = stdout.channel.recv_exit_status()
    output = stdout.read().decode("utf-8")
    err = stderr.read().decode("utf-8")
    return (code, output, err)

  def close(self):
    self.client.close()


PROCESS_OUT_OF_SPACE_CODE = 1
CONTAINER_OUT_OF_SPACE_CODE = 125


class Task(threading.Thread):
  client: Client
  cpu: int
  error: Optional[str]
  folder: str
  lock: threading.Lock
  memory: int
  node_ip: str
  pending_queue: collections.deque
  results_folder: str
  task: ec2_util.Task
  timeout: int

  def __init__(self, results_folder: str, node_ip: str, key: str, folder: str, pending_queue: collections.deque, timeout: int, memory: int):
    super(Task, self).__init__()
    self.cpu = 170
    self.error = None
    self.task = pending_queue.popleft()
    self.folder = folder
    self.lock = threading.Lock()
    self.memory = memory*1024*1024*1024
    self.node_ip = node_ip
    self.nonce = random.randint(1, 100*1000)
    self.pending_queue = pending_queue
    self.results_folder = results_folder
    self.timeout = timeout
    self.__setup__(key + ".pem")

  def __setup__(self, pem: str):
    self.client = Client(self.node_ip, pem, self.timeout)

  def shutdown(self):
    self.lock.acquire()
    if self.client is not None:
      stop_code, _, _ = self.client.exec_command("sudo docker rm {0:d}".format(self.nonce))
      if stop_code != 0:
        print("Unexpected stop code", stop_code)
      self.client.close()
      self.client = None
    self.lock.release()

  def running(self):
    if self.client is None:
      return False

    c = "sudo docker ps -a --filter=name={0:d}".format(self.nonce)
    _, output, _ = self.client.exec_command(c)
    output = output.split("\n")
    done = len(output) < 2 or " Exited " in output[1]
    if done:
      self.shutdown()
    return not done

  def run(self):
    print("RUNNING", self.task.key)
    start_time: float = time.time()
    c: str = "sudo docker run --name {0:d} -m {1:d} --cpu-shares {2:d} -v /home/ubuntu/Docker/app:/home/ubuntu/app app ".format(self.nonce, self.memory, self.cpu)
    c += "python3 main.py --key {0:s}".format(self.task.key)
    code, output, err = self.client.exec_command(c)
    print(code, output, err)
    end_time: float = time.time()
    if code != 0:
      if code not in [PROCESS_OUT_OF_SPACE_CODE, CONTAINER_OUT_OF_SPACE_CODE]:
        print("CODE:", code)
        print("OUTPUT:", output)
        print("ERROR:", err)
        self.error = err
      self.pending_queue.appendleft(self.task)
    else:
      s3 = boto3.resource("s3")
      bucket = s3.Bucket("maccoss-ec2")
      objs = list(bucket.objects.filter(Prefix="/".join(self.task.key.split("/")[:2])))
      if len(objs) != 1:
        print("Cannot find output for", self.task.key, len(objs))
        self.error = "Cannot find output for " + self.task.key
        return
      print("Finished task", self.task.key)
      with open("{0:s}/tasks/{1:f}-{2:f}".format(self.results_folder, start_time, end_time), "w+") as f:
        f.write("S3 CREATED TIME: {0:f}\n".format(self.task.created_at))
        f.write("RECEIVED TIME {0:f}\n".format(self.task.received_at))
        f.write("EXECUTION START TIME: {0:f}\n".format(start_time))
        f.write("EXECUTION END TIME: {0:f}\n".format(end_time))
        f.write("KEY NAME: {0:s}\n".format(self.task.key))

    self.shutdown()


class Setup(threading.Thread):
  def __init__(self, node):
    super(Setup, self).__init__()
    self.node = node

  def __create_instance__(self, tag_name: str, params: Dict[str, Any]):
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

  def __get_credentials__(self) -> Tuple[str, str]:
    home = os.path.expanduser("~")
    f = open("{0:s}/.aws/credentials".format(home))
    lines = f.readlines()
    header = "[default]"
    for i in range(len(lines)):
      if lines[i].strip() == header:
        access_key = lines[i + 1].split("=")[1].strip()
        secret_key = lines[i + 2].split("=")[1].strip()
        break
    return (access_key, secret_key)

  def run(self):
    node = self.__create_instance__("emr-node-{0:f}".format(time.time()), self.node.params)
    node.reload()
    client = Client(node.public_ip_address, self.node.params["key"] + ".pem", self.node.params["timeout"])
    access, secret = self.__get_credentials__()
    client.exec_command('echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/.aws/credentials'.format(access, secret))
    client.exec_command('echo "[default]\naws_access_key_id={0:s}\naws_secret_access_key={1:s}" >> ~/Docker/app/credentials'.format(access, secret))
    client.exec_command('echo -e "{0:s}\n{1:s}\n\n\n\n\n\nY\ny\n" | s3cmd --configure'.format(access, secret))
    code, stdout, err = client.exec_command("cd ~/Docker/app; s3cmd get {0:s}/ . --recursive --force".format(self.node.s3_application_url))
    code, output, err = client.exec_command("cd ~/Docker; sudo docker build --tag=app .")
    if code != 0:
      print(output)
      print(err)
    assert(code == 0)
    _, stdout, _ = client.exec_command("grep -c ^processor /proc/cpuinfo")
    self.node.num_cpus = int(stdout.strip())
    client.close()
    self.node.node = node


class Node:
  cpu_utilization: float
  create_time: float
  error: Optional[str]
  folder: str
  memory_utilization: float
  num_cpus: int
  node: Optional[Any]
  node_id: int
  num_tasks: int
  params: Dict[str, Any]
  pending_queue: collections.deque
  results_folder: str
  state: str
  s3_application_url: str
  tasks: List[Task]

  def __init__(self, node_id: int, s3_application_url: str, pending_queue: collections.deque, results_folder: str, params: Dict[str, Any]):
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
    if len(self.pending_queue) > 0:
      self.tasks.append(Task(self.results_folder, self.node.public_ip_address, self.params["key"], self.folder, self.pending_queue, self.params["timeout"]), self.params["memory"])
      self.tasks[-1].start()
    self.num_tasks = len(self.tasks)

  def __get_metrics__(self):
    metrics = [0.0, 0.0]
    code, output, err = self.client.exec_command('sudo docker stats --format "table {{.CPUPerc}}\t{{.MemPerc}}" --no-stream')
    lines = list(filter(lambda line: len(line) > 0, output.split("\n")[1:]))
    for line in lines:
      m = STATS_REGEX.search(line)
      if m is None:
        print("wtf", output)
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
      self.client = Client(self.node.public_ip_address, self.params["key"] + ".pem", self.params["timeout"])
      if self.state == "STARTING":
        self.state = "RUNNING"
      elif self.state == "TERMINATING":
        self.node.terminate()
      self.start_time = time.time()

    i = 0
    while i < len(self.tasks):
      if not self.tasks[i].running():
        if self.tasks[i].error is not None:
          print(self.node.instance_id, "ERROR", self.tasks[i].error)
          self.error = self.tasks[i].error
        self.tasks.pop(i)
      else:
        i += 1

    self.num_tasks = len(self.tasks)
    if self.state in ["TERMINATING", "TERMINATED"]:
      print(self.node.instance_id, "Terminating: Tasks left", self.num_tasks)
      if self.state == "TERMINATING" and self.node is not None and self.num_tasks == 0:
        self.__terminate__()

    if self.state != "TERMINATED":
      [cpu, mem] = self.__get_metrics__()
      self.cpu_utilization = cpu
      self.memory_utilization = mem
      print(self.node.instance_id, "CPU Utilization", self.cpu_utilization, "Memory", self.memory_utilization)

  def terminate(self):
    self.terminate_time = time.time()
    self.state = "TERMINATING"

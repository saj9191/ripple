import boto3
import botocore
from database.database import Database, Entry, Table, Statistics
import requests
from typing import Any, BinaryIO, Dict, List, Optional, Union


class Object(Entry):
  def __init__(self, key: str, resources: Any, statistics: Statistics):
    Entry.__init__(self, key, resources, statistics)

  def __download__(self, f: BinaryIO) -> int:
    self.resources.download_fileobj(f)
    return f.tell()

  def __get_content__(self) -> bytes:
    return self.resources.get()["Body"].read()

  def __get_range__(self, start_index: int, end_index: int) -> bytes:
    return self.resources.get(Range="bytes={0:d}-{1:d}".format(start_index, end_index))["Body"].read()

  def content_length(self) -> int:
    return self.resources.content_length

  def get_metadata(self) -> Dict[str, str]:
    return self.resources.metadata

  def last_modified_at(self) -> float:
    return self.resources.last_modified.timestamp()


class Bucket(Table):
  def __init__(self, name: str, statistics: Statistics, resources: Any):
    Table.__init__(self, name, statistics, resources)
    self.bucket = self.resources.Bucket(name)


class S3(Database):
  def __init__(self, params):
    self.s3 = boto3.resource("s3")
    self.client = boto3.client("lambda")
    self.params = params
    self.sleep_time = 1
    Database.__init__(self)

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    bucket = self.s3.Bucket(table_name)
    bucket.download_fileobj(key, f)
    return f.tell()

  def __get_content__(self, table_name: str, key: str, start_byte: int, end_byte: int) -> bytes:
    obj = self.s3.Object(table_name, key)
    return obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read()

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    done = False
    bucket = self.s3.Bucket(table_name)
    while not done:
      try:
        if prefix:
          objects = bucket.objects.filter(Prefix=prefix)
        else:
          objects = bucket.objects.all()
        objects = list(map(lambda obj: Object(obj.key, self.s3.Object(table_name, obj.key), self.statistics), objects))
        done = True
        self.sleep_time = min(max(int(self.sleep_time / 2), 1), self.max_sleep_time)
      except Exception as e:
        time.sleep(self.sleep_time)
        self.sleep_time *= 2

    return objects

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str], invoke=True):
    self.__s3_write__(table_name, key, content, metadata)

  def __read__(self, table_name: str, key: str) -> bytes:
    obj = self.s3.Object(table_name, key)
    content = obj.get()["Body"].read()
    return content

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str], invoke: bool):
    self.__s3_write__(table_name, key, content, metadata, invoke)

  def __s3_write__(self, table_name: str, key: str, content: Union[bytes, BinaryIO], metadata: Dict[str, str], invoke: bool):
    done: bool = False
    while not done:
      try:
        self.s3.Object(table_name, key).put(Body=content, Metadata=metadata)
        self.sleep_time = min(max(int(self.sleep_time / 2), 1), self.max_sleep_time)
        done = True
      except botocore.exceptions.ClientError as e:
        print("Warning: S3::write Rate Limited. Sleeping for", self.sleep_time)
        time.sleep(self.sleep_time)
        self.sleep_time *= 2

    if "output_function" in self.params and invoke:
      payload = {
        "Records": [{
          "s3": {
            "bucket": {
              "name": table_name
            },
            "object": {
              "key": key
            },
            "ancestry": self.params["ancestry"],
          },
        }]
      }
      if "reexecute" in self.params:
        payload["execute"] = self.params["reexecute"]
      self.payloads.append(payload)
      self.invoke(self.params["output_function"], payload)

  def contains(self, table_name: str, key: str) -> bool:
    try:
      self.s3.Object(table_name, key).load()
      return True
    except Exception:
      return False

  def get_entry(self, table_name: str, key: str) -> Optional[Object]:
    return Object(key, self.s3.Object(table_name, key), self.statistics)

  def get_table(self, table_name: str) -> Table:
    return Table(table_name, self.statistics, self.s3)

  def create_payload(self, table_name: str, key: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": table_name
          },
          "object": {
            "key": key
          },
          "extra_params": extra,
          "ancestry": self.params["ancestry"],
        }
      }]
    }

    if "reexecute" in self.params:
      payload["execute"] = self.params["reexecute"]
    return payload

  def invoke(self, name, payload):
    if self.params["provider"] == "lambda":
      self.payloads.append(payload)
      response = self.client.invoke(
        FunctionName=name,
        InvocationType="Event",
        Payload=json.JSONEncoder().encode(payload)
      )
      assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)
    else:
      raise Exception("s3::invoke: Unknown provider", self.params["provider"])

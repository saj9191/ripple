import boto3
import botocore
import json
import os
import random
import time
from typing import Any, BinaryIO, Dict, List, Optional, Union


class Statistics:
  list_count: int
  read_byte_count: int
  read_count: int
  write_byte_count: int
  write_count: int

  def __init__(self):
    self.list_count = 0
    self.read_byte_count = 0
    self.read_count = 0
    self.write_byte_count = 0
    self.write_count = 0


class Entry:
  key: str
  resources: Any
  statistics: Optional[Statistics]

  def __init__(self, key: str, resources: Any, statistics: Optional[Statistics]):
    self.key = key
    self.resources = resources
    self.statistics = statistics

  def __download__(self, f: BinaryIO) -> int:
    raise Exception("Entry::__download__ not implemented")

  def content_length(self) -> int:
    raise Exception("Entry::content_length not implemented")

  def download(self, f: BinaryIO) -> int:
    content_length: int = self.__download__(f)
    return content_length

  def get_content(self) -> str:
    raise Exception("Entry::get_content not implemented")

  def get_metadata(self) -> Dict[str, str]:
    raise Exception("Entry::get_metadata not implemented")

  def get_range(self, start_index: int, end_index: int) -> str:
    raise Exception("Entry::get_range not implemented")

  def last_modified_at(self) -> float:
    raise Exception("Entry::last_modified_at not implemented")


class Table:
  name: str
  resources: Any
  statistics: Statistics

  def __init__(self, name: str, statistics: Statistics, resources: Any):
    self.name = name
    self.resources = resources
    self.statistics = statistics


class Database:
  statistics: Statistics

  def __init__(self):
    self.statistics = Statistics()

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    raise Exception("Database::__download__ not implemented")

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    raise Exception("Database::__get_entries__ not implemented")

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str]):
    raise Exception("Database::__put__ not implemented")

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    raise Exception("Database::__write__ not implemented")

  def contains(self, table_name: str, key: str) -> bool:
    raise Exception("Database::contains not implemented")

  def download(self, table_name: str, key: str, f: BinaryIO) -> int:
    content_length: int = self.__download__(table_name, key, f)
    return content_length

  def get(self, table_name: str, key: str) -> Any:
    raise Exception("Database::get not implemented")

  def get_entry(self, table_name: str, key: str) -> Optional[Entry]:
    raise Exception("Database::key not implemented")

  def get_entries(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    self.statistics.list_count += 1
    return self.__get_entries__(table_name, prefix)

  def get_table(self, table_name: str) -> Table:
    raise Exception("Database::get_table not implemented")

  def invoke(self, client, name, params, payload):
    raise Exception("Database::invoke not implemented")

  def put(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str]):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += os.path.getsize(content.name)
    self.__put__(table_name, key, content, metadata)

  def read(self, table_name: str, key: str) -> str:
    self.statistics.read_count += 1
    content: str = self.__read__(table_name, key)
    self.statistics.read_byte_count += len(content)
    return content

  def write(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += len(content)
    self.__write__(table_name, key, content, metadata)


class Object(Entry):
  def __init__(self, key: str, statistics: Statistics, resources: Any):
    Entry.__init__(self, key, statistics, resources)

  def __download__(self, f: BinaryIO) -> int:
    self.resources.download_fileobj(f)
    return f.tell()

  def content_length(self) -> int:
    return self.resources.content_length

  def get_content(self) -> str:
    return self.resources.get()["Body"].decode("utf-8")

  def get_metadata(self) -> Dict[str, str]:
    return self.resources.metadata

  def get_range(self, start_index: int, end_index: int) -> str:
    return self.resources.get(Range="bytes={0:d}-{1:d}".format(start_index, end_index))["Body"].read().decode("utf-8")

  def last_modified_at(self) -> float:
    return self.resources.last_modified.timestamp()


class Bucket(Table):
  def __init__(self, name: str, statistics: Statistics, resources: Any):
    Table.__init__(self, name, statistics, resources)
    self.bucket = self.resources.Bucket(name)


class S3(Database):
  payloads: List[Dict[str, Any]]

  def __init__(self):
    self.s3 = boto3.resource("s3")
    Database.__init__(self)

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    bucket = self.s3.Bucket(table_name)
    bucket.download_fileobj(key, f)
    return f.tell()

  def __get_content__(self, table_name: str, key: str, start_byte: int, end_byte: int) -> str:
    obj = self.s3.Object(table_name, key)
    content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read()
    return content.decode("utf-8")

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    done = False
    while not done:
      try:
        if prefix:
          objects = self.s3.Bucket.objects.filter(Prefix=prefix)
        else:
          objects = self.s3.Bucket.objects.all()
        done = True
      except Exception as e:
        time.sleep(1)

    objects = list(map(lambda obj: Object(obj.key, self.statistics, obj), objects))
    return objects

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str]):
    self.__s3_write__(table_name, key, content, metadata)

  def __read__(self, table_name: str, key: str) -> str:
    obj = self.s3.Object(table_name, key)
    content = obj.get()["Body"].read()
    return content.decode("utf-8")

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    self.__s3_write__(table_name, key, content, metadata)

  def __s3_write__(self, table_name: str, key: str, content: Union[bytes, BinaryIO], metadata: Dict[str, str]):
    done: bool = False
    while not done:
      try:
        self.s3.Object(table_name, key).put(Body=content, Metadata=metadata)
        done = True
      except botocore.exceptions.ClientError as e:
        print("Warning: S3::write Rate Limited")
        time.sleep(random.randint(1, 10))

    self.payloads.append({
      "Records": [{
        "s3": {
          "bucket": {
            "name": table_name
          },
          "object": {
            "key": key
          }
        }
      }]
    })

  def contains(self, table_name: str, key: str) -> bool:
    try:
      self.s3.Object(table_name, key).load()
      return True
    except Exception:
      return False

  def get_entry(self, table_name: str, key: str) -> Optional[Object]:
    return Object(key, self.statistics, self.s3.Object(table_name, key))

  def get_table(self, table_name: str) -> Table:
    return Table(table_name, self.statistics, self.s3)

  def invoke(self, client, name, params, payload):
    self.payloads.append(payload)
    response = client.invoke(
      FunctionName=name,
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

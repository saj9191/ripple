import boto3
import botocore
import random
import time
from typing import Any, BinaryIO, Dict, List, Optional


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


class Database:
  statistics: Statistics

  def __init__(self):
    self.statistics = Statistics()

  def __download__(self, bucket_name: str, key: str, f: BinaryIO) -> int:
    raise Exception("Database::__download__ not implemented")

  def __get_objects__(self, bucket_name: str, prefix: Optional[str]=None) -> List[Any]:
    raise Exception("Database::__get_entries__ not implemented")

  def __put__(self, bucket_name: str, key: str, f: BinaryIO, metadata: Dict[str, str]):
    raise Exception("Database::__put__ not implemented")

  def __read__(self, bucket_name: str, key: str) -> str:
    raise Exception("Database::__read__ not implemented")

  def __write__(self, bucket_name: str, key: str, content: bytes):
    raise Exception("Database::__write__ not implemented")

  def contains(self, bucket_name: str, key: str) -> bool:
    raise Exception("Database::contains not implemented")

  def download(self, bucket_name: str, key: str, f: BinaryIO) -> int:
    content_length: int = self.__download__(bucket_name, key, f)
    return content_length

  def get_object(self, bucket_name: str, key: str) -> Optional[Any]:
    raise Exception("Database::key not implemented")

  def get(self, bucket_name: str, key: str) -> Any:
    raise Exception("Database::get not implemented")

  def get_content(self, bucket_name: str, key: str, start_byte: int, end_byte: int) -> str:
    self.statistics.read_count += 1
    content: str = self.__get_content__(bucket_name, key, start_byte, end_byte)
    self.statistics.read_byte_count += len(content)
    return content

  def get_objects(self, bucket_name: str, prefix: Optional[str]=None) -> List[Any]:
    self.statistics.list_count += 1
    return self.__get_entries__(table, prefix)

  def put(self, bucket_name: str, key: str, f: BinaryIO, metadata: Dict[str, str]):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += f.tell()
    self.__put__(bucket_name, key, f, metadata)

  def read(self, bucket_name: str, key: str) -> str:
    self.statistics.read_count += 1
    content: str = self.__read__(table, key)
    self.statistics.read_byte_count += len(content)
    return content

  def write(self, bucket_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += len(content)
    self.__write__(bucket_name, key, content, metadata)


class S3(Database):
  payloads: List[Dict[str, Any]]

  def __init__(self):
    self.s3 = boto3.resource("s3")
    Database.__init__(self)

  def __download__(self, bucket_name: str, key: str, f: BinaryIO) -> int:
    bucket = self.s3.Bucket(table)
    bucket.download_fileobj(key, f)
    return f.tell()

  def __get_content__(self, bucket_name: str, key: str, start_byte: int, end_byte: int) -> str:
    obj = self.s3.Object(table, key)
    content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read()
    return content.decode("utf-8")

  def __get_objects__(self, bucket_name: str, prefix: Optional[str]=None) -> List[Any]:
    done = False
    while not done:
      try:
        if prefix:
          objects = list(self.s3.Bucket.objects.filter(Prefix=prefix))
        else:
          objects = list(self.s3.Bucket.objects.all())
        done = True
      except Exception as e:
        time.sleep(1)
    return objects

  def __put__(self, bucket_name: str, key: str, f: BinaryIO, metadata: Dict[str, str]):
    self.__write__(table, key, f, metadata)

  def __read__(self, bucket_name: str, key: str) -> str:
    obj = self.s3.Object(table, key)
    content = obj.get()["Body"].read()
    return content.decode("utf-8")

  def __write__(self, bucket_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    done: bool = False
    while not done:
      try:
        self.s3.Object(table, key).put(Body=content, Metadata=metadata)
        done = True
      except botocore.exceptions.ClientError as e:
        print("Warning: S3::write Rate Limited")
        time.sleep(random.randint(1, 10))

    payloads.append({
      "Records": [{
        "s3": {
          "bucket": {
            "name": table
          },
          "object": {
            "key": key
          }
        }
      }]
    })

  def contains(self, bucket_name: str, key: str) -> bool:
    try:
      self.s3.Object(table, key).load()
      return True
    except Exception:
      return False

  def get_object(self, bucket_name: str, key: str) -> Optional[Any]:
    return self.s3.Object(table, key)

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

  def calculate_total_cost(self) -> float:
    list_cost: float = (self.list_count / 1000.0) * 0.005
    read_cost: float = (self.read_count / 1000.0) * 0.004
    write_cost: float = (self.write_count / 1000.0) * 0.005
    print("List cost:", list_cost)
    print("Read cost:", read_cost)
    print("Write cost:", write_cost)
    total_cost: float = list_cost + read_cost + write_cost
    print("Total cost:", total_cost)
    return total_cost


class Entry:
  key: str
  resources: Any
  statistics: Statistics

  def __init__(self, key: str, resources: Any, statistics: Statistics):
    self.key = key
    self.resources = resources
    self.statistics = statistics

  def __download__(self, f: BinaryIO) -> int:
    raise Exception("Entry::__download__ not implemented")

  def __get_content__(self) -> bytes:
    raise Exception("Entry::__get_content__ not implemented")

  def __get_range__(self, start_index: int, end_index: int) -> bytes:
    raise Exception("Entry::get_range not implemented")

  def content_length(self) -> int:
    raise Exception("Entry::content_length not implemented")

  def download(self, f: BinaryIO) -> int:
    count = 0
    done = False
    content_length: int = -1
    while not done:
      self.statistics.read_count += 1
      try:
        content_length = self.__download__(f)
        done = True
      except Exception as e:
        count += 1
        if count == 3:
          raise e
    return content_length

  def get_content(self) -> bytes:
    self.statistics.read_count += 1
    return self.__get_content__()

  def get_metadata(self) -> Dict[str, str]:
    raise Exception("Entry::get_metadata not implemented")

  def get_range(self, start_index: int, end_index: int) -> bytes:
    self.statistics.read_count += 1
    return self.__get_range__(start_index, end_index)

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
  payloads: List[Dict[str, Any]]
  statistics: Statistics

  def __init__(self):
    self.payloads = []
    self.statistics = Statistics()
    self.max_sleep_time = 5

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    raise Exception("Database::__download__ not implemented")

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    raise Exception("Database::__get_entries__ not implemented")

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str], invoke: bool):
    raise Exception("Database::__put__ not implemented")

  def __read__(self, table_name: str, key: str) -> bytes:
    raise Exception("Database::__read__ not implemented")

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str], invoke: bool):
    raise Exception("Database::__write__ not implemented")

  def contains(self, table_name: str, key: str) -> bool:
    raise Exception("Database::contains not implemented")

  def create_payload(self, table_name: str, key: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    raise Exception("Database::create_payload not implemented")

  def download(self, table_name: str, key: str, file_name: str) -> int:
    count = 0
    done = False
    content_length: int = -1
    while not done:
      self.statistics.read_count += 1
      try:
        with open(file_name, "wb+") as f:
          content_length = self.__download__(table_name, key, f)
          done = True
      except Exception as e:
        count += 1
        if count == 3:
          raise e
    return content_length

  def get_entry(self, table_name: str, key: str) -> Optional[Entry]:
    raise Exception("Database::key not implemented")

  def get_entries(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    self.statistics.list_count += 1
    return self.__get_entries__(table_name, prefix)

  def get_statistics(self) -> Dict[str, Any]:
    return {
      "payloads": self.payloads,
      "read_count": self.statistics.read_count,
      "write_count": self.statistics.write_count,
      "list_count": self.statistics.list_count,
      "write_byte_count": self.statistics.write_byte_count,
      "read_byte_count": self.statistics.read_byte_count,
    }

  def get_table(self, table_name: str) -> Table:
    raise Exception("Database::get_table not implemented")

  def invoke(self, name, payload):
    raise Exception("Database::invoke not implemented")

  def put(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str], invoke: bool):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += os.path.getsize(content.name)
    self.__put__(table_name, key, content, metadata, invoke)

  def read(self, table_name: str, key: str) -> bytes:
    self.statistics.read_count += 1
    content: bytes = self.__read__(table_name, key)
    self.statistics.read_byte_count += len(content)
    return content

  def write(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str], invoke: bool):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += len(content)
    self.__write__(table_name, key, content, metadata, invoke)

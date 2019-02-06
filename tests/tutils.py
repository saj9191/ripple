from typing import Any, BinaryIO, List, Dict


def equal_lists(list1, list2):
  s1 = set(list1)
  s2 = set(list2)
  return len(s1.intersection(s2)) == len(s1) and len(s2.intersection(s1)) == len(s1)


class S3:
  def __init__(self, buckets):
    self.buckets = {}
    for bucket in buckets:
      self.buckets[bucket.name] = bucket

  def Bucket(self, bucket_name):
    return self.buckets[bucket_name]

  def Object(self, bucket_name: str, key: str):
    objs = list(self.buckets[bucket_name].objects.filter(Prefix=key))
    if len(objs) == 0:
      obj = Object(key, bucket_name=bucket_name)
      self.buckets[bucket_name].objects.objects.append(obj)
      return obj
    return objs[0]


class Bucket:
  def __init__(self, name, objects):
    self.name = name
    self.objects = Objects(objects)

  def objects(self):
    return self.objects

  def download_fileobj(self, key, f):
    obj = list(self.objects.filter(Prefix=key))[0]
    f.write(str.encode(obj.content))


class Objects:
  def __init__(self, objects):
    self.objects = objects

  def all(self):
    return self.objects

  def filter(self, Prefix):
    return filter(lambda o: o.key.startswith(Prefix), self.objects)


class Object:
  def __init__(self, key: str, content: str="", last_modified: int=0, bucket_name="bucket", metadata: Dict[str, str]={}):
    self.bucket_name = bucket_name
    self.key = key
    self.content = content
    self.metadata = metadata
    self.last_modified = last_modified
    self.content_length = len(content)

  def download_fileobj(self, f: BinaryIO):
    f.write(str.encode(self.content))

  def get(self, Range: str=""):
    if len(Range) == 0:
      return {"Body": Content(self.content)}
    parts = Range.split("=")[1].split("-")
    start = int(parts[0])
    end = min(int(parts[1]), self.content_length - 1)
    return {"Body": Content(self.content[start:end + 1])}

  def load(self):
    raise Exception("")

  def put(self, Body: str="", Metadata: Dict[str, str]={}, StorageClass: str=""):
    self.metadata = Metadata
    if type(Body) == str:
      self.content = Body
    elif type(Body) == bytes:
      self.content = Body.decode("utf-8")
    else:
      self.content = Body.read().decode("utf-8")


class Content:
  def __init__(self, content: str):
    self.content = content

  def read(self):
    return str.encode(self.content)


class Context:
  def __init__(self, milliseconds_left: int):
    self.milliseconds_left = milliseconds_left

  def get_remaining_time_in_millis(self):
    return self.milliseconds_left


class Client:
  def __init__(self):
    self.invokes = []

  def invoke(self, FunctionName: str, InvocationType: str, Payload: Dict[str, Any]):
    self.invokes.append({
      "name": FunctionName,
      "type": InvocationType,
      "payload": Payload
    })

    return {
      "ResponseMetadata": {
        "HTTPStatusCode": 202
      }
    }


def create_event(bucket_name: str, key: str, buckets: List[Bucket], params: Dict[str, Any], offsets=None):
  def load():
    return params

  return {
    "test": True,
    "client": Client(),
    "load_func": load,
    "s3": S3(buckets),
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket_name,
        },
        "object": {
          "key": key,
        },
        "extra_params": {
          "offsets": offsets if offsets else []
        }
      }
    }]
  }


def create_context(params):
  return Context(params["timeout"])

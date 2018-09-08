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


class Bucket:
  def __init__(self, name, objects):
    self.name = name
    self.objects = Objects(objects)

  def objects(self):
    return self.objects


class Objects:
  def __init__(self, objects):
    self.objects = objects

  def all(self):
    return self.objects

  def filter(self, Prefix):
    return filter(lambda o: o.name.startswith(Prefix), self.objects)


class Object:
  def __init__(self, name, content=""):
    self.name = name
    self.content = content
    self.content_length = len(content)

  def get(self, Range):
    parts = Range.split("=")[1].split("-")
    start = int(parts[0])
    end = min(int(parts[1]), self.content_length - 1)
    return {"Body": Content(self.content[start : end + 1])}


class Content:
  def __init__(self, content):
    self.content = content

  def read(self):
    return str.encode(self.content)

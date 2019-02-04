import time

class Task:
  key: str
  created_at: float
  received_at: float

  def __init__(self, key: str, timestamp: float):
    self.key = key
    self.created_at = timestamp
    self.received_at = time.time()



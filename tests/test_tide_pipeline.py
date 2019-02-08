import json
import pipeline
import unittest
from typing import Any, Dict


class Tide(unittest.TestCase):
  def test_basic(self):
    with open("../json/basic-tide.json") as f:
      params: Dict[str, Any] = json.loads(f.read())

    pp: pipeline.Pipeline = pipeline.Pipeline(params)
    with open("../data/test.mzML") as f:
      content: str = f.read()

    pp.run("0/123.400000-13/1-1/1-1-3-suffix.new", content)


if __name__ == "__main__":
  unittest.main()

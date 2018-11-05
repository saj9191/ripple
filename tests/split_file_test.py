import inspect
import json
import os
import sys
import unittest
import tutils
from tutils import Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/lambda")
import split_file
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\nG H I\nJ K L\nM N O\nP Q R\n"
object1 = Object("0/123.400000-13/1/1-1-0-suffix.new_line", content)
bucket1 = Bucket("bucket1", [object1])
log = Bucket("log", [])
input_format = util.parse_file_name(object1.key)
output_format = dict(input_format)
output_format["prefix"] = 1

params = {
  "file": "split_file",
  "format": "new_line",
  "log": "log",
  "output_function": "an-output-function",
  "ranges": False,
  "split_size": 20,
}


def get_payload(bucket_name, key, prefix, offsets=None):
  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": bucket_name
        },
        "object": {
          "key": key,
        },
        "extra_params": {
          "prefix": prefix,
        },
      }
    }]
  }

  if offsets is not None:
    payload["Records"][0]["s3"]["offsets"] = {
      "offsets": offsets
    }
  return payload


def get_invoke(name, bucket_name, key, prefix, offsets, file_id, num_files, bin=None):
  payload = get_payload(bucket_name, key, prefix, offsets)
  if bin is None:
    payload["Records"][0]["s3"]["object"]["file_id"] = file_id
    payload["Records"][0]["s3"]["object"]["num_files"] = num_files

  return {
    "name": name,
    "type": "Event",
    "payload": json.JSONEncoder().encode(payload),
  }


class SplitFunction(unittest.TestCase):
  def check_payload_equality(self, expected_invokes, actual_invokes):
    self.assertEqual(len(expected_invokes), len(actual_invokes))
    for i in range(len(expected_invokes)):
      expected_invokes[i]["payload"] = json.JSONDecoder().decode(expected_invokes[i]["payload"])
      actual_invokes[i]["payload"] = json.JSONDecoder().decode(actual_invokes[i]["payload"])
      self.assertDictEqual(expected_invokes[i], actual_invokes[i])

  def test_basic(self):
    event = tutils.create_event(bucket1.name, object1.key)
    context = tutils.create_context(params, [bucket1, log])
    split_file.handler(event, context)

    invoke1 = get_invoke("an-output-function", bucket1.name, object1.key, prefix=1, offsets=[0, 19], file_id=1, num_files=2)
    invoke2 = get_invoke("an-output-function", bucket1.name, object1.key, prefix=1, offsets=[20, 35], file_id=2, num_files=2)

    expected_invokes = [invoke1, invoke2]
    self.check_payload_equality(expected_invokes, context["client"].invokes)


if __name__ == "__main__":
  unittest.main()

import inspect
import json
import os
import sys
import unittest
from tutils import S3, Bucket, Object, Context, Client

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/lambda")
import split_file
sys.path.insert(0, parentdir + "/formats")

content = "A B C\nD E F\nG H I\nJ K L\nM N O\nP Q R\n"
object1 = Object("0/123.400000-13/1/1-0-suffix.methyl", content)
bucket1 = Bucket("bucket1", [object1])
input_format = util.parse_file_name(object1.name)
output_format = dict(input_format)
output_format["prefix"] = 1

params = {
  "test": True,
  "s3": S3([bucket1]),
  "ranges": False,
  "batch_size": 3,
  "chunk_size": 20,
  "token": 45,
  "format": "methyl",
  "output_function": "an-output-function",
  "bucket_format": dict(input_format),
}


class SplitFunction(unittest.TestCase):
  def test_basic(self):
    params["client"] = Client()
    params["context"] = Context(50*1000)
    client = params["client"]

    split_file.split_file(bucket1.name, object1.name, input_format, output_format, {}, params)
    self.assertEqual(len(client.invokes), 2)

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket1.name
          },
          "object": {
            "key": object1.name,
          },
          "extra_params": {
            "token": 45,
            "prefix": 1
          }
        }
      }]
    }

    payload1 = dict(payload)
    payload1["Records"][0]["s3"]["object"]["file_id"] = 1
    payload1["Records"][0]["s3"]["object"]["more"] = True
    payload1["Records"][0]["s3"]["offsets"] = {
      "offsets": [0, 17]
    }

    invoke1 = {
      "name": "an-output-function",
      "type": "Event",
      "payload": json.JSONEncoder().encode(payload1),
    }

    payload2 = dict(payload)
    payload2["Records"][0]["s3"]["object"]["file_id"] = 2
    payload2["Records"][0]["s3"]["object"]["more"] = False
    payload1["Records"][0]["s3"]["offsets"] = {
      "offsets": [18, 35]
    }

    invoke2 = {
      "name": "an-output-function",
      "type": "Event",
      "payload": json.JSONEncoder().encode(payload2),
    }

    expected_invokes = [invoke1, invoke2]
    self.assertEqual(len(expected_invokes), len(client.invokes))
    for i in range(len(expected_invokes)):
      client.invokes[i]["payload"] = json.JSONDecoder().decode(client.invokes[i]["payload"])
      expected_invokes[i]["payload"] = json.JSONDecoder().decode(expected_invokes[i]["payload"])
      self.assertDictEqual(expected_invokes[i], client.invokes[i])

  def test_next_invoke_trigger(self):
    params["client"] = Client()
    params["context"] = Context(1000)
    params["name"] = "split-file"
    client = params["client"]

    input_format = util.parse_file_name(object1.name)
    output_format = dict(input_format)
    output_format["prefix"] = 1
    split_file.split_file(bucket1.name, object1.name, input_format, output_format, {}, params)
    self.assertEqual(len(client.invokes), 1)

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket1.name
          },
          "object": {
            "key": object1.name,
            "file_id": 1,
            "more": True,
          },
          "extra_params": {
            "token": 45,
            "prefix": 0,
            "file_id": 1,
            "id": 1,
          },
          "offsets": {
            "offsets": [0, 17],
          }
        }
      }]
    }

    invoke = {
      "name": "split-file",
      "type": "Event",
      "payload": json.JSONEncoder().encode(payload),
    }

    expected_invokes = [invoke]
    self.assertEqual(len(expected_invokes), len(client.invokes))
    for i in range(len(expected_invokes)):
      client.invokes[i]["payload"] = json.JSONDecoder().decode(client.invokes[i]["payload"])
      expected_invokes[i]["payload"] = json.JSONDecoder().decode(expected_invokes[i]["payload"])
      self.assertDictEqual(expected_invokes[i], client.invokes[i])


if __name__ == "__main__":
  unittest.main()

import json
import split_file
import tutils
import unittest
import util
from tutils import TestDatabase


def get_invoke(name, bucket_name, key, prefix, offsets, file_id, num_files):
  payload = tutils.create_payload(bucket_name, key, prefix, file_id, num_files, offsets)
  payload["Records"][0]["s3"]["ancestry"] = [("123.400000-13", 0, 1, 1, 1, 1)]
  payload["execute"] = 0.0
  payload["log"] = ["123.400000-13", 1, 1, 1, 1, file_id]

  return json.JSONDecoder().decode(json.JSONEncoder().encode(payload))


class SplitFunction(unittest.TestCase):
  def check_payload_equality(self, expected_invokes, actual_invokes):
    self.assertEqual(len(expected_invokes), len(actual_invokes))
    actual_invokes = sorted(actual_invokes, key=lambda p: p["log"])
    expected_invokes = sorted(actual_invokes, key=lambda p: p["log"])
    for i in range(len(expected_invokes)):
      self.assertDictEqual(expected_invokes[i], actual_invokes[i])

  def test_basic(self):
    params = {
      "bucket": "table1",
      "file": "split_file",
      "format": "new_line",
      "log": "log",
      "name": "split",
      "output_function": "an-output-function",
      "ranges": False,
      "split_size": 20,
      "timeout": 60,
    }
    database = TestDatabase(params)
    table1 = database.create_table(params["bucket"])
    log = database.create_table(params["log"])
    entry1 = table1.add_entry("0/123.400000-13/1-1/1-0.0000-1-suffix.txt", "A B C\nD E F\nG H I\nJ K L\nM N O\nP Q R\n")
    input_format = util.parse_file_name(entry1.key)
    output_format = dict(input_format)
    output_format["prefix"] = 1

    event = tutils.create_event(database, table1.name, entry1.key)
    context = tutils.create_context(params)
    split_file.handler(event, context)

    invoke1 = get_invoke("an-output-function", table1.name, entry1.key, prefix=1, offsets=[0, 19], file_id=1, num_files=2)
    invoke2 = get_invoke("an-output-function", table1.name, entry1.key, prefix=1, offsets=[20, 35], file_id=2, num_files=2)

    expected_invokes = [invoke1, invoke2]
    self.check_payload_equality(expected_invokes, database.payloads)


if __name__ == "__main__":
  unittest.main()

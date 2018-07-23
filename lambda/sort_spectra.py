import boto3
import constants
import json
import spectra
import util
import xml.etree.ElementTree as ET


def sort_spectra(bucket_name, key, start_byte, end_byte, file_id, more, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))

  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  content = obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8").strip()

  index = content.rindex(constants.CLOSING_TAG)
  content = content[:index + len(constants.CLOSING_TAG)]
  root = ET.fromstring("<data>" + content + "</data>")
  mzml = str.encode(spectra.mzMLSpectraIterator.create(list(root.iter("spectrum")), True))
  if more:
    byte_id = start_byte
  else:
    byte_id = obj.content_length

  subset_key = util.file_name(ts, nonce, file_id,  byte_id, obj.content_length, m["ext"])
  s3.Object(params["output_bucket"], subset_key).put(Body=mzml)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  params = json.loads(open("sort_spectra.json").read())
  start_byte = event["Records"][0]["s3"]["range"]["start_byte"]
  end_byte = event["Records"][0]["s3"]["range"]["end_byte"]
  file_id = event["Records"][0]["s3"]["range"]["file_id"]
  more = event["Records"][0]["s3"]["range"]["more"]
  sort_spectra(bucket_name, key, start_byte, end_byte, file_id, more, params)

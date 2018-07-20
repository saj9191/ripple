import boto3
import json
import spectra
import util


def split_spectra(key, bucket_name, batch_size, chunk_size):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  m = util.parse_file_name(key)
  ext = m["ext"]

  if ext == "mzML":
    iterator = spectra.mzMLSpectraIterator(obj, batch_size, chunk_size)
  elif ext == "ms2":
    iterator = spectra.ms2SpectraIterator(obj, batch_size, chunk_size)

  client = boto3.client("lambda")
  more = True
  file_id = 1
  while more:
    [start_byte, end_byte, more] = iterator.nextOffsets()
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name
          },
          "object": {
            "key": key
          },
          "range": {
            "file_id": file_id,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "more": False
          }
        }
      }]
    }
    file_id += 1

    # TODO: Check responses?
    client.invoke(
      FunctionName="AnalyzeSpectra",
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )

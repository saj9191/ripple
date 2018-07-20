import boto3
import s3
import util

def split_spectra(key, bucket_name, batch_size, chunk_size):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  num_bytes = obj.content_length

  m = util.parse_file_name(key)
  ts = m["timestamp"]
  _, ext = os.path.splitext(key)

  if ext == ".mzML":
    iterator = spectra.mzMLSpectraIterator(obj, batch_size, chunk_size)
  elif ext == ".ms2":
    iterator = spectra.ms2SpectraIterator(obj, batch_size, chunk_size)

  client = botot3.client("lambda")

  more = True:
  while more:
    [start_byte, end_byte, more] = iterator.nextOffsets(self)
    payload = b"""{
      Records: [{
        "s3": {
          "bucket": {
            "name": {0:s}
          },
          "object": {
            "key": {1:s}
          },
          "range": {
            "start_byte": {2:d},
            "end_byte": {3:d}
          }
        }
      }]
    }""".format(bucket_name, key, start_byte_end_byte)

    # TODO: Check responses?
    client.invoke(
      FunctionName="AnalyzeSpectra",
      InvocationType="Event",
      Payload=payload
    )


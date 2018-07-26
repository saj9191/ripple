import boto3
import json
import spectra
import split
import util


def split_spectra(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  print("TIMESTAMP {0:f} NONCE {1:d}".format(m["timestamp"], m["nonce"]))
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]
  split.split_spectra(key, bucket_name, batch_size, chunk_size)


def bucket_split_spectra(bucket_name, key, params):
  util.clear_tmp()
  m = util.parse_file_name(key)
  ts = m["timestamp"]
  nonce = m["nonce"]
  print("TIMESTAMP {0:f} NONCE {1:d}".format(ts, nonce))
  batch_size = params["batch_size"]
  chunk_size = params["chunk_size"]
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)

  m = util.parse_file_name(key)
  ext = m["ext"]

  if ext == "mzML":
    iterator = spectra.mzMLSpectraIterator(obj, batch_size, chunk_size)
  else:
    raise Exception("Implement")

  more = True
  file_id = 0
  buckets = []
  buckets_to_count = 0
  for i in range(15):
    buckets.append([])
    buckets_to_count[i] = 0

  while more:
    file_id += 1
    [s, more] = iterator.next()
    for spectrum in s:
      bucket = int(spectrum[0] / 100)
      buckets[bucket].append(spectrum[1])

    for i in range(len(buckets)):
      bucket = buckets[i]
      if len(bucket) > 0:
        buckets_to_count[i] += 1
        file_id = buckets_to_count[i]
        bucket_key = util.file_name(ts, nonce, file_id, file_id, obj.content_length, params["ext"], i)
        s3.Object(params["output_bucket"], bucket_key).put(Body=str.encode(spectra.mzMLSpectraIterator.create(buckets[i])))

  client = boto3.client("lambda")
  for i in range(len(buckets)):
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket_name
          },
          "object": {
            "bucket_id": i,
            "num_files": buckets_to_count[i],
            "timestamp": ts,
            "nonce": nonce,
          }
        }
      }]
    }
    response = client.invoke(
      FunctionName="SortSpectra",
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)


def handler(event, context):
  bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
  key = event["Records"][0]["s3"]["object"]["key"]
  #sort = event["Records"][0]["s3"]["object"]["sort"]
  params = json.loads(open("split_spectra.json").read())
  split_spectra(bucket_name, key, params)
  #if sort:
  #  bucket_split_spectra(bucket_name, key, params)
  #else:

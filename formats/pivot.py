import boto3


def combine(bucket_name, keys, temp_name, params):
  s3 = boto3.resource("s3")
  pivots = []

  file_key = None
  for key in keys:
    obj = s3.Object(bucket_name, key)
    content = obj.get()["Body"].read().decode("utf8")
    [file_bucket, file_key, pivot_content] = content.split("\n")
    new_pivots = map(lambda p: int(p), pivot_content.split("\t"))
    pivots += new_pivots
  assert(file_key is not None)

  pivots = sorted(pivots)
  super_pivots = []
  index = 0
  num_bins = len(pivots) / len(keys)
  increment = int(len(pivots) / num_bins)
  while index < len(pivots) - 1:
    super_pivots.append(pivots[index])
    index += increment

  super_pivots.append(pivots[-1])
  spivots = list(map(lambda p: str(p), super_pivots))
  content = "{0:s}\n{1:s}\n{2:s}".format(file_bucket, file_key, "\t".join(spivots))
  with open(temp_name, "w+") as f:
    f.write(content)


def get_pivot_ranges(bucket_name, key, bucket_prefix):
  s3 = boto3.resource("s3")
  ranges = []

  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read().decode("utf-8")
  [file_bucket, file_key, pivot_content] = content.split("\n")
  pivots = list(map(lambda p: int(p), pivot_content.split("\t")))

  for i in range(len(pivots) - 1):
    ranges.append({
      "range": [pivots[i], pivots[i + 1]],
      "bucket": "{0:s}-{1:d}".format(bucket_prefix, i)
    })

  return file_bucket, file_key, ranges

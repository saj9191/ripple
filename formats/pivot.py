import boto3
import iterator


class Iterator(iterator.Iterator):
  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, Iterator, obj, batch_size, chunk_size)

  def combine(bucket_name, keys, temp_name, params):
    s3 = boto3.resource("s3")
    pivots = []

    file_key = None
    for key in keys:
      obj = s3.Object(bucket_name, key)
      content = obj.get()["Body"].read().decode("utf8")
      [file_bucket, file_key, pivot_content] = content.split("\n")
      new_pivots = list(map(lambda p: float(p), pivot_content.split("\t")))
      pivots += new_pivots
    assert(file_key is not None)

    pivots = sorted(pivots)
    super_pivots = []
    num_bins = params["num_bins"]
    increment = int(len(pivots) / num_bins)
    super_pivots = pivots[0::increment]
    super_pivots[-1] = pivots[-1]

    spivots = list(map(lambda p: str(p), super_pivots))
    content = "{0:s}\n{1:s}\n{2:s}".format(file_bucket, file_key, "\t".join(spivots))
    with open(temp_name, "w+") as f:
      f.write(content)

  def next(self, identifier=False):
    [start_byte, end_byte, more] = self.nextOffsets()
    if start_byte == -1:
      return [[], more]
    return [Iterator.get(self.obj, start_byte, end_byte, identifier), more]


def get_pivot_ranges(bucket_name, key, bucket_prefix, num_buckets):
  s3 = boto3.resource("s3")
  ranges = []

  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read().decode("utf-8")
  [file_bucket, file_key, pivot_content] = content.split("\n")
  pivots = list(map(lambda p: float(p), pivot_content.split("\t")))

  for i in range(len(pivots) - 1):
    end_range = int(pivots[i + 1])
    ranges.append({
      "range": [int(pivots[i]), end_range],
      "bucket": "{0:s}-{1:d}".format(bucket_prefix, (i % num_buckets) + 1),
      "bin": i + 1,
    })

  return file_bucket, file_key, ranges

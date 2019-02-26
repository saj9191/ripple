import boto3
import heapq
import numpy as np
import pywren
import time
from PIL import Image, ImageFilter
from sklearn.neighbors import NearestNeighbors


def get_train_data(offsets):
  s3 = boto3.resource("s3")
  token = b"\r\n"
  chunk_size = 1000
  obj = s3.Object("maccoss-spacenet", "train.classification.w1-h1")
  content = obj.get(Range="bytes={0:d}-{1:d}".format(offsets[0], offsets[1]))["Body"].read()
  index = content.rindex(token)
  content = content[:index]
  if not content.startswith(token):
    prefix = obj.get(Range="bytes={0:d}-{1:d}".format(offsets[0] - chunk_size, offsets[0]-1))["Body"].read()
    index = prefix.rindex(token)
    content = prefix[index + len(token):] + content

  lines = content.split(b"\r\n")
  train_data = []
  classifications = []
  for i in range(len(lines)):
    line = lines[i]
    if len(line) > 0:
      parts = line.split(b" ")
      classification = int(parts[-1])
      x = b" ".join(parts[:-1])
      features = np.frombuffer(x, dtype=int)
      train_data.append(features)
      classifications.append(classification)

  return [train_data, classifications]


def get_features(px, x, y, window_height, window_width, height, width):
  features = []
  for iy in range(-window_height, window_height + 1):
    for ix in range(-window_width, window_width + 1):
      wy = y + iy
      wx = x + ix
      if 0 <= wy and wy < height and 0 < wx and wx < width:
        [r, g, b] = px[wx, wy]
        features += [r, g, b]
      else:
        features += [255, 255, 255]
  assert(len(features) == 27)
  return features


def get_test_data(name, key, pixels_per_bin):
  im = Image.open(key)
  px = im.load()
  width, height = im.size
  ranges = []
  for i in range(0, width*height, pixels_per_bin):
    ranges.append((name, i, min(i + pixels_per_bin, width * height)))
  return ranges


def process_test_data(test_ranges):
  [key, start_index, end_index] = test_ranges
  s3 = boto3.resource("s3")
  temp_file = "/tmp/" + key
  with open(temp_file, "wb+") as f:
    s3.Object("maccoss-spacenet-log", key).download_fileobj(f)

  bim = Image.open(temp_file)
#  bim = im.filter(ImageFilter.BoxBlur(1))
  px = bim.load()
  width, height = bim.size
  window_height = 1
  window_width = 1
  test_data = []
  count = 0

  for i in range(start_index, end_index):
    y = int(i / width)
    x = i % width
    f = get_features(px, x, y, window_height, window_width, height, width)
    test_data.append((x, y, f))
  return test_data


def pair_with_training_data(test_data, split_size):
  pairs = []
  s3 = boto3.resource("s3")
  obj = s3.Object("maccoss-spacenet", "train.classification.w1-h1")
  content_length = obj.content_length
  num_files = int((content_length + split_size - 1) / split_size)

  for count in range(len(test_data)):
    for file_id in range(num_files):
      offsets = [file_id * split_size, min(content_length, (file_id + 1) * split_size) - 1]
      pairs.append((test_data[count], offsets))

  return pairs


def knn(data):
  [test_ranges, offsets] = data
  test_data = process_test_data(test_ranges)
  [train_data, classifications] = get_train_data(offsets)
  features = list(map(lambda d: d[2], test_data))
  neigh = NearestNeighbors(n_neighbors=100, algorithm="brute")
  neigh.fit(features)
  [distances, indices] = neigh.kneighbors(features)
  results = []
  for i in range(len(distances)):
    pairs = []
    for j in range(len(distances[i])):
      pairs.append((distances[i][j], classifications[indices[i][j]]))
    results.append(("{0:d} {1:d}".format(test_data[i][0], test_data[i][1]), pairs))
  return results


def classify(neighbors):
  scores = [0, 0, 0]
  neighbors = sorted(neighbors, key=lambda n: n[0])
  d1 = -1 * neighbors[-1][0]
  dk = -1 * neighbors[0][0]
  denom = dk - d1 if dk != d1 else 0.0001
  assert(d1 <= dk)
  for j in range(len(neighbors)):
    distance = -1 * neighbors[j][0]
    w = 1 if j == 0 else (dk - distance) / denom
    c = neighbors[j][1]
    scores[c] += w
  m = max(scores)
  top = [i for i, j in enumerate(scores) if j == m]
  if len(top) == 1:
    return top[0]
  return 2


def draw(key, classifications):
  im = Image.open(key)
  im.load()
  width, height = im.size
  print("Width * height", width * height)
  print("num class", len(classifications))
  for y in range(height):
    for x in range(width):
      if classifications[y * width + x] == 1:
        im.putpixel((x, y), (255, 0, 0))
  im.save(key)



def preprocess(name, key, pixels_per_bin, split_size):
  test_data = get_test_data(name, key, pixels_per_bin)
  print("Num test chunks", len(test_data))
  return pair_with_training_data(test_data, split_size)


def mmap(pwex, func, data):
  futures = pwex.map(func, data)
  res = pywren.get_all_results(futures)
  return res


def print_results(times, batches, count):
  print("Count", count, "Batches", batches)
  print("Average Preprocess", times["preprocess"] / float(count))
  print("Average Map", times["map"] / float(count))
  print("Average Wait Time", times["wait"] / float(count))
  print("Average Combine", times["combine"] / float(count))
  print("Average Render TIme", times["render"] / float(count))
  print("Total", times["total"] / float(count))


def main():
  pixels_per_bin = 1000
  split_size = 10000000
  num_items_per_batch = 10000
  k = 100
  pwex = pywren.default_executor(job_max_runtime=600)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("maccoss-spacenet-log")
  objs = list(bucket.objects.all())
  count = 0
  times = {"combine": 0, "map": 0, "preprocess": 0, "total": 0, "wait": 0, "render": 0}

  for obj in objs[:10]:
    print("Processing", obj.key, count)
    count += 1
    key = "test.tif"
    with open(key, "wb+") as f:
      s3.Object("maccoss-spacenet-log", obj.key).download_fileobj(f)

    start_time = time.time()
    st = time.time()
    data = preprocess(obj.key, key, pixels_per_bin, split_size)
    et = time.time()
    print("Preprocess", et - st)
    times["preprocess"] += (et - st)

    nearest_neighbors = {}
    batches = int((len(data) + num_items_per_batch - 1) / num_items_per_batch)
    print("Num data points", len(data))
    not_dones = []
    for i in range(batches):
      b = data[i * num_items_per_batch : min((i + 1) * num_items_per_batch, len(data))]
      print(i, batches, len(b))
      done = False
      st = time.time()
      not_dones += pwex.map(knn, b)
      et = time.time()
      print("Map", i, et - st)
      times["map"] += (et - st) 

      while not done:
        s = time.time()
        [dones, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
        e = time.time()
        times["wait"] += (e - s)
        print("Wait", e - s, "Done", len(dones), "Not done", len(not_dones))
        st = time.time()
        for result in dones:
          points = result.result()
          for [s, neighbors] in points:
            if s not in nearest_neighbors:
              nearest_neighbors[s] = neighbors
              heapq.heapify(nearest_neighbors[s])
            else:
              for neighbor in neighbors:
                [distance, classification] = neighbor
                if len(nearest_neighbors[s]) < k or -1*distance > nearest_neighbors[s][0][0]:
                  heapq.heappush(nearest_neighbors[s], (-1*distance, classification))
                if len(nearest_neighbors[s]) > k:
                  heapq.heappop(nearest_neighbors[s])
        done = (len(not_dones) == 0)
        et = time.time()
        print("Combine", i, et - st)
        times["combine"] += (et - st)

    st = time.time()
    im = Image.open(key)
    for s in nearest_neighbors.keys():
      [x, y] = list(map(lambda i: int(i), s.split(" ")))
      if classify(nearest_neighbors[s]) == 1:
        im.putpixel((x, y), (255, 0, 0))
    im.save(key)
    et = time.time()
    end_time = time.time()
    times["total"] += (end_time - start_time)
    print_results(times, batches, count)

main()

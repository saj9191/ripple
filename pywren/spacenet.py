import boto3
import numpy as np
import pywren
import time
from PIL import Image, ImageFilter
from sklearn.neighbors import NearestNeighbors


def get_train_data(bucket_name, key):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  content = obj.get()["Body"].read()
  lines = content.split(b"\r\n")
  train_data = []
  train_results = []
  for i in range(len(lines)):
    line = lines[i]
    if len(line) > 0:
      parts = line.split(b" ")
      classification = int(parts[-1])
      x = b" ".join(parts[:-1])
      features = np.frombuffer(x, dtype=int)
      train_data.append(features)
      train_results.append(classification)

  return [train_data, train_results]


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


def get_test_data(bucket_name, key):
  s3 = boto3.resource("s3")
  obj = s3.Object(bucket_name, key)
  with open(key, "wb+") as f:
    obj.download_fileobj(f)
  im = Image.open(key)
  bim = im.filter(ImageFilter.BoxBlur(1))
  px = bim.load()
  width, height = bim.size
  window_height = 1
  window_width = 1
  test_data = [[]]
  max_concurrency = 10000
  num_items = int((width * height + max_concurrency - 1) / max_concurrency)
  count = 0

  for y in range(height):
    for x in range(width):
      f = get_features(px, x, y, window_height, window_width, height, width)
      if len(test_data[count]) == num_items:
        test_data.append([])
        count += 1
      test_data[count].append(f)
  return test_data


def classify(test_data, classifications, distances, indices):
  results = np.empty(len(test_data), dtype=int)
  assert(len(test_data) == len(distances))
  for i in range(len(distances)):
    scores = [0, 0, 0]
    d1 = distances[i][0]
    dk = distances[i][-1]
    denom = dk - d1 if dk != d1 else 0.0001
    assert(d1 <= dk)
    for j in range(len(distances[i])):
      w = 1 if j == 0 else (dk - distances[i][j]) / denom
      c = classifications[indices[i][j]]
      scores[c] += w
      m = max(scores)
      top = [i for i, j in enumerate(scores) if j == m]
      if len(top) == 1:
        results[i] = top[0]
      else:
        results[i] = 2
  return results


def knn(test_data):
  [train_data, classifications] = get_train_data("maccoss-spacenet", "train.classification.w1-h1")
  neigh = NearestNeighbors(n_neighbors=100, algorithm="brute")
  neigh.fit(train_data)
  [distances, indices] = neigh.kneighbors(test_data)
  return classify(test_data, classifications, distances, indices)


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


def main():
  start_time = time.time()
  #pwex = pywren.local_executor()
  #pwex = pywren.standalone_executor(max_idle_time=30)
  pwex = pywren.default_executor(job_max_runtime=300)
  key = "3band_AOI_1_RIO_img1.tif"
  test_data = get_test_data("maccoss-spacenet-log", key)
  print("Fetched test data", len(test_data))
  futures = pwex.map(knn, test_data)
  res = pywren.get_all_results(futures)
  classifications = []
  for r in res:
    classifications += r.flatten().tolist()
  print("class", classifications[0:2])
  print(len(classifications))
  draw(key, classifications)
  end_time = time.time()
  print("Total Time", end_time - start_time)


main()

import argparse
import boto3
import heapq
import json
import os
import numpy as np
import pywren
import subprocess
import time
from PIL import Image, ImageFilter
from sklearn.neighbors import NearestNeighbors


def get_features(im, x, y, window_height, window_width, height, width):
  features = np.zeros([2 * window_height + 1, 2 * window_width + 1, 3], dtype=int)
  for index_y in range(2 * window_height + 1):
    window_y = y - window_height + index_y
    for index_x in range(2 * window_width + 1):
      window_x = x - window_width + index_x
      if 0 <= window_y and window_y < height and 0 <= window_x and window_x < width:
        features[index_y][index_x] = im[window_x, window_y]
      else:
        features[index_y][index_x] = [255, 255, 255]
  return features


def convert_to_pixels(data):
  [key, pixels_per_bin] = data
  s3 = boto3.resource("s3")
  file_name = "/tmp/" + key
  with open(file_name, "wb+") as f:
    s3.Object("maccoss-spacenet-log", key).download_fileobj(f)

  im = Image.open(file_name)
  px = im.load()
  width, height = im.size

  num_bins = int((width * height + pixels_per_bin - 1) / pixels_per_bin)
  bin_id = 1
  name = "{0:d}-{1:d}.pixel".format(bin_id, num_bins)
  f = open("/tmp/" + name, "wb+")
  names = []
  names.append("job/" + name)

  for y in range(height):
    for x in range(width):
      if y != 0 or x != 0:
        f.write(b"\r\n")
      features = get_features(px, x, y, 1, 1, height, width)
      f.write(str.encode("{x} {y} ".format(x=x, y=y)) + features.tostring())
      if ((y * width + x) % pixels_per_bin) == (pixels_per_bin - 1):
        f.close()
        s3.Object("maccoss-ec2", "job/" + name).put(Body=open("/tmp/" + name, "rb"))
        subprocess.call("rm /tmp/" + name, shell=True)
        bin_id += 1
        name = "{0:d}-{1:d}.pixel".format(bin_id, num_bins)
        f = open("/tmp/" + name, "wb+")
        names.append("job/" + name)
  f.close()
  s3.Object("maccoss-ec2", "job/" + name).put(Body=open("/tmp/" + name, "rb"))
  subprocess.call("rm /tmp/" + name, shell=True)
  subprocess.call("rm " + file_name, shell=True)
  return names


def pair(data):
  [key, split_size] = data
  pairs = []
  s3 = boto3.resource("s3")
  obj = s3.Object("maccoss-spacenet", "train.classification.w1-h1")
  content_length = obj.content_length
  num_files = int((content_length + split_size - 1) / split_size)

  for file_id in range(num_files):
    offsets = [file_id * split_size, min(content_length, (file_id + 1) * split_size) - 1]
    pairs.append([key, offsets])

  return pairs


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


def process_test_data(key):
  s3 = boto3.resource("s3")
  temp_file = "/tmp/train"
  with open(temp_file, "wb+") as f:
    s3.Object("maccoss-ec2", key).download_fileobj(f)

  with open(temp_file, "rb") as f:
    lines = f.read().split(b"\r\n")

  test_data = []
  for line in lines:
    if len(line) > 0:
      parts = line.split(b" ")
      test_data.append([int(parts[0]), int(parts[1]), np.frombuffer(b" ".join(parts[2:]), dtype=int)])
  
  subprocess.call("rm /tmp/train", shell=True)
  return test_data

def get_test_data(name, key, pixels_per_bin):
  im = Image.open(key)
  px = im.load()
  width, height = im.size
  ranges = []
  for i in range(0, width*height, pixels_per_bin):
    ranges.append((name, i, min(i + pixels_per_bin, width * height)))
  return ranges


def knn(data):
  [key, offsets] = data
  test_data = process_test_data(key)
  [train_data, classifications] = get_train_data(offsets)
  features = list(map(lambda d: d[2], test_data))
  neigh = NearestNeighbors(n_neighbors=100, algorithm="brute")
  neigh.fit(features)
  window_height = window_width = 1
  [distances, indices] = neigh.kneighbors(features)
  features = np.zeros([2 * window_height + 1, 2 * window_width + 1, 3], dtype=int)
  results = []
  for i in range(len(distances)):
    [x, y, f] = test_data[i]
    neighbors = np.zeros([100, 2], dtype=float)
    for j in range(len(distances[i])):
      neighbors[j][0] = distances[i][j]
      neighbors[j][1] = classifications[indices[i][j]]
    results.append(str.encode("{x} {y} ".format(x=x, y=y)) + neighbors.tostring())

  s3 = boto3.resource("s3")
  name = "knn/{0:d}-{1:d}/{2:s}".format(offsets[0], offsets[1], key)
  s3.Object("maccoss-ec2", name).put(Body=b"\r\n\r\n".join(results))
  return name


def combine_bins(subset):
  s3 = boto3.resource("s3")
  top_scores = {}
  for key in subset:
    content = s3.Object("maccoss-ec2", key).get()["Body"].read()
    results = content.split(b"\r\n\r\n")
    for result in results:
      if len(result) > 0:
	parts = result.split(b" ")
	s = b" ".join(parts[:2])
	features = np.frombuffer(b" ".join(parts[2:]), dtype=float)
	features = np.reshape(features, [len(features) / 2, 2])
	features = list(map(lambda x: [-1*x[0], x[1]], features))
	if s not in top_scores:
          heapq.heapify(features)
	  top_scores[s] = features
	else:
	  for [d, c] in features:
	    heapq.heappush(top_scores[s], [d, c])
	while len(top_scores[s]) > 100:
	  heapq.heappop(top_scores[s])
  tmp_file = "/tmp/combine"
  keys = list(top_scores.keys())
  with open(tmp_file, "wb+") as f:
    for i in range(len(keys)):
      if i > 0:
	f.write(b"\n")
      s = keys[i]
      line = s
      assert(len(top_scores[s]) <= 100)
      for [score, c] in top_scores[s]:
	assert(0 <= c and c <= 2)
	line += str.encode(",{0:f} {1:d}".format(-1 * score, int(c)))
      f.write(line)

  parts = key.split("/")
  name = "combine/" + parts[-1]
  s3.Object("maccoss-ec2", name).put(Body=open(tmp_file, "rb"))
  subprocess.call("rm " + tmp_file, shell=True)
  return [name]


def combine(keys):
  s3 = boto3.resource("s3")
  tmp_file = "/tmp/combine"
  with open(tmp_file, "wb+") as f:
    for i in range(len(keys)):
      key = keys[i]
      if i > 0:
	f.write(b"\n") 
      f.write(s3.Object("maccoss-ec2", key).get()["Body"].read())
  name = "combine/combine"
  s3.Object("maccoss-ec2", name).put(Body=open(tmp_file, "rb"))
  subprocess.call("rm " + tmp_file, shell=True)
  return [name]
  

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
    c = int(neighbors[j][1])
    if c < 0 or c > 2:
      print(neighbors)
    scores[c] += w
  m = max(scores)
  top = [i for i, j in enumerate(scores) if j == m]
  if len(top) == 1:
    return top[0]
  return 2


def draw_borders(data):
  [im_key, knn_key] = data
  s3 = boto3.resource("s3")
  file_name = "/tmp/image.tif"
  with open(file_name, "wb+") as f:
    s3.Object("maccoss-spacenet-log", im_key).download_fileobj(f)

  im = Image.open(file_name)
  im.load()
  width, height = im.size
  classifications = np.empty([height, width], dtype=int)
  
  chunk_size = 1000*1000*1000
  obj = s3.Object("maccoss-ec2", knn_key)
  content_length = obj.content_length
  start_index = 0
  remainder = b""
  while start_index < content_length:
    end_index = min(obj.content_length - 1, start_index + chunk_size)
    content = remainder + obj.get(Range="bytes={0:d}-{1:d}".format(start_index, end_index))["Body"].read()
    rindex = content.rfind(b"\n")
    x = min(rindex + 1, len(content))

    remainder = content[x:]
    content = content[:x]
    lines = content.split(b"\n")

    for line in lines:
      if len(line) > 0:
        parts = list(map(lambda p: list(map(lambda l: float(l), p.split(" "))), line.split(",")))
        [x, y] = parts[0]
        if classify(parts[1:]) == 1:
          im.putpixel((int(x), int(y)), (255, 0, 0))
    start_index = end_index + 1

  im.save(file_name)
  s3.Object("maccoss-ec2", "results/" + im_key).put(Body=open(file_name, "rb"))
  subprocess.call("rm " + file_name, shell=True)
  return ["results/" + im_key]


def preprocess(name, key, pixels_per_bin, split_size):
  test_data = get_test_data(name, key, pixels_per_bin)
  print("Num test chunks", len(test_data))
  return pair_with_training_data(test_data, split_size)


def mmap(pwex, func, data):
  futures = pwex.map(func, data)
  res = pywren.get_all_results(futures)
  return res


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--output_folder", type=str, help="Folder to store results in")
  args = parser.parse_args()
  if not os.path.isdir(args.output_folder):
    os.mkdir(args.output_folder)
  pwex = pywren.default_executor(job_max_runtime=600)
  s3 = boto3.resource("s3")
  bucket = s3.Bucket("maccoss-spacenet-log")
#  objs = list(bucket.objects.all())
  objs = list(bucket.objects.filter(Prefix="3band_AOI_1_RIO_img1457.tif"))
  count = 12
  stats = []
  start_time = time.time()
  run(pwex, objs[0], stats[-1])
  return
  for i in range(count):
    sleep = max(start_time + 5*60*i - time.time(), 0)
    print(i, "Sleep for", sleep)
    time.sleep(sleep)
    stats.append([])
#    stats.append({"convert": [], "pair": [], "knn": [], "combine1": [], "combine2": [], "draw": []})
    run(pwex, objs[i], stats[-1])

  with open(args.output_folder + "/statistics", "w+") as f:
    f.write(json.dumps(stats))

def run(pwex, obj, stats):
  s3 = boto3.resource("s3")
  pixels_per_bin = 1000
  split_size = 10000000

  start_time = time.time()
  print("Convert")
  st = time.time()
  not_dones = pwex.map(convert_to_pixels, [[obj.key, pixels_per_bin]])
  dones = []
  while len(not_dones) > 0:
    [d, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
    end_time = time.time()
    stats += list(map(lambda s: [st, end_time], d))
    dones += d
  assert(len(dones) == 1)
  results = dones[0].result()
  et = time.time()
  print("Done convert", et - st)
  # Pair
  print("Pair")
  st = time.time()
  results = list(map(lambda r: [r, split_size], results))
  not_dones = pwex.map(pair, results)
  dones = []
  while len(not_dones) > 0:
    [d, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
    end_time = time.time()
    stats += list(map(lambda s: [st, end_time], d))
    dones += d
  pairings = []
  for r in dones:
    s = r.result()
    pairings += s
  et = time.time()
  print("Done pair", et - st)
  # KNN
  print("KNN")
  st = time.time()
  not_dones = pwex.map(knn, pairings)
  dones = []
  while len(not_dones) > 0:
    [d, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
    end_time = time.time()
    stats += list(map(lambda s: [st, end_time], d))
    dones += d
    print("dones", len(dones), "not dones", len(not_dones))

  key_to_set = {}
  for r in dones:
    s = r.result()
    p = s.split("/")
    key = p[-1]
    if key not in key_to_set:
      key_to_set[key] = []
    key_to_set[key].append(s)
  et = time.time()
  print("Up to combine", et - st)

  print("First combine")
  st = time.time()
  to_combine = list(key_to_set.values())
  not_dones = pwex.map(combine_bins, to_combine)
  dones = []
  while len(not_dones) > 0:
    [d, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
    end_time = time.time()
    stats += list(map(lambda s: [st, end_time], d))
    dones += d
    print("dones", len(dones), "not dones", len(not_dones))

  results = []
  for r in dones:
    s = r.result()
    results += s 
  et = time.time()
  print("Including sub-combine", et - st)

  print("Second combine")
  st = time.time()
  results = combine(results)
  results = list(map(lambda r: [obj.key, r], results))
  et = time.time()
  print("Including combine", et - st)

  print("Borders")
  st = time.time()
  draw_borders(results[0])
  not_dones = pwex.map(draw_borders, results)
  dones = []
  while len(not_dones) > 0:
    [d, not_dones] = pywren.wait(not_dones, pywren.ANY_COMPLETED, 64, 10)
    end_time = time.time()
    stats += list(map(lambda s: [st, end_time], d))
    dones += d
  et = time.time()
  assert(len(dones) == 1)
  dones[0].result()
  print("Including drawing", et - st)
  end_time = time.time()
  print("Total time", end_time - start_time)

main()

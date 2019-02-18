import argparse
import boto3
import numpy as np
import os
import random
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

SOLUTION_REGEX = re.compile("([A-Za-z0-9_]+),([0-9]+),\"POLYGON \(([0-9\-\.\, \(\)]+)\).*")
POLYGON_REGEX = re.compile("([0-9\.]+) ([-0-9\.]+) ([0-9]+)")
INSIDE = 0
BORDER = 1
OUTSIDE = 2


def parse_polygon(p):
  poly = []
  for (x, y, clz) in POLYGON_REGEX.findall(p):
    poly.append([float(x), float(y)])
  assert(len(poly) > 1)
  poly = poly[:-1]
  return poly


def parse_solutions(file_name):
  f = open(file_name)
  solutions = {}
  for line in f.readlines()[1:]:
    m = SOLUTION_REGEX.match(line)
    if m is None:
      assert("EMPTY" in line)
      name = line.split(",")[0]
      solutions[name] = []
    else:
      name = m.group(1)
      if name not in solutions:
        solutions[name] = []
      solutions[name].append(parse_polygon(m.group(3)))
  return solutions


# https://stackoverflow.com/questions/328107/how-can-you-determine-a-point-is-between-two-other-points-on-a-line-segment
def on_border(polygon, x, y):
  for i in range(len(polygon)):
    j = (i + 1) % len(polygon)
    [ix, iy] = polygon[i]
    [jx, jy] = polygon[j]
    if (x == ix and y == iy) or (x == jx and y == jy):
      return True

    cross_prod = (y - iy) * (jx - ix) - (x - ix) * (jy - iy)
    if abs(cross_prod) > 10:
      continue

    dot_prod = (x - ix) * (jx - ix) + (y - iy) * (jy - iy)
    if dot_prod < 0:
      continue

    squared_length_ba = (jx - ix) ** 2 + (jy - iy) ** 2
    if dot_prod > squared_length_ba:
      continue

    return True
  return False


# https://stackoverflow.com/questions/217578/how-can-i-determine-whether-a-2d-point-is-within-a-polygon
def in_polygon(polygon, x, y):
  poly_x = list(map(lambda p: p[0], polygon))
  poly_y = list(map(lambda p: p[1], polygon))
  min_x = min(poly_x)
  max_x = max(poly_x)
  min_y = min(poly_y)
  max_y = max(poly_y)
  inside = False

  if x < min_x or x > max_x or y < min_y or y > max_y:
    return False

  i = 0
  j = len(polygon) - 1
  while i < len(polygon):
    [ix, iy] = polygon[i]
    [jx, jy] = polygon[j]
    if (iy > y) != (jy > y) and (x < (jx - ix) * (y - iy) / (jy - iy) + ix):
      inside = not inside
    j = i
    i += 1

  return inside


def write_classification(f, pixels):
  for p in pixels:
    f.write(p)


def create_window(im, x, y, window_width, window_height, width, height):
  window = np.zeros([2 * window_height + 1, 2 * window_width + 1, 3], dtype=int)
  for index_y in range(2 * window_height + 1):
    window_y = y - window_height + index_y
    if 0 <= window_y and window_y < height:
      for index_x in range(2 * window_width + 1):
        window_x = x - window_width + index_x
        if 0 <= window_x and window_x < width:
          window[index_y][index_x] = im[window_y, window_x]

  return window


def write(pixels, window, clz):
  s = window.tostring()
  pixels.add(s + str.encode(" {c}\n\n".format(c=clz)))


def create_classifications(s3, folder, image_name, polygons, border, inside, outside, window_width, window_height):
  im = plt.imread(folder + "/" + image_name)
  [height, width, dim] = im.shape

  for y in range(height):
    for x in range(width):
      [r, g, b] = im[y, x]
      window = create_window(im, x, y, window_width, window_height, width, height)
      if len(polygons) == 0:
        write(outside, window, OUTSIDE)
      else:
        found = False
        for polygon in polygons:
          if on_border(polygon, x, y):
            write(border, window, BORDER)
            found = True
            break
          elif in_polygon(polygon, x, y):
            write(inside, window, INSIDE)
            found = True
        if not found:
          write(outside, window, OUTSIDE)


def process_images(folder, solutions, bucket, width, height):
  image_names = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
  random.shuffle(image_names)
  image_names = image_names[:500]
  border = set()
  inside = set()
  outside = set()
  s3 = boto3.resource("s3")

  for i in range(len(image_names)):
    print(i + 1, "/", len(image_names), "B", len(border), "I", len(inside), "O", len(outside))
    image_name = image_names[i]
    name = image_name.replace("3band_", "").replace(".tif", "")
    solution = solutions[name]
    create_classifications(s3, folder, image_name, solution, border, inside, outside, width, height)

  border = list(border)
  inside = list(inside)
  outside = list(outside)
  num_points = min([len(border), len(inside), len(outside)])
  random.shuffle(border)
  random.shuffle(inside)
  random.shuffle(outside)
  print("Num points", num_points)
  classifications = border[:num_points] + inside[:num_points] + outside[:num_points]
  random.shuffle(classifications)
  key = "train.classification.w{w}-h{h}".format(w=width, h=height)
  temp_name = "/tmp/{0:s}".format(key)
  with open(temp_name, "wb+") as f:
    write_classification(f, classifications)
  s3.Object(bucket, key).put(Body=open(temp_name, "rb"))
  print("Final", "Border", len(border), "Inside", len(inside), "Outside", len(outside))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket", type=str, required=True, help="Bucket to upload results")
  parser.add_argument("--width", type=int, required=True, help="Window width")
  parser.add_argument("--height", type=int, required=True, help="Window height")
  args = parser.parse_args()

  solutions = parse_solutions("../spacenet_data/vector_data/summarydata/AOI_1_RIO_polygons_solution_3band.csv")
  process_images("../spacenet_data/train_data/3band", solutions, args.bucket, args.width, args.height)


main()

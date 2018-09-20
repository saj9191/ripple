import json
import matplotlib.pyplot as plt
import os
import random
import re
import time
import util

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
  for [r, g, b, clz] in pixels:
    f.write("{r} {g} {b} {c}\n".format(r=r, g=g, b=b, c=clz))


def create_classifications(s3, folder, image_name, polygons, border, inside, outside):
  im = plt.imread(folder + "/" + image_name)
  [height, width, dim] = im.shape
  border = []
  inside = []
  outside = []

  for y in range(height):
    for x in range(width):
      [r, g, b] = im[y, x]
      if len(polygons) == 0:
        outside.append([x, y])
      else:
        found = False
        for polygon in polygons:
          if on_border(polygon, x, y):
            border.append([r, g, b, BORDER])
            found = True
            break
          elif in_polygon(polygon, x, y):
            inside.append([r, g, b, INSIDE])
            found = True
        if not found:
          outside.append([r, g, b, OUTSIDE])


def process_images(folder, solutions, params):
  s3 = util.s3(params)
  image_names = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
  random.shuffle(image_names)
  image_names = image_names[:200]
  border = set()
  inside = set()
  outside = set()
  total_files = 100
  num_items_per_file = 100 * 1000
  third = num_items_per_file / 3
  file_id = 1

  i = 0
  for image_name in image_names:
    print(time.time(), i, image_name)
    name = image_name.replace("3band_", "").replace(".tif", "")
    solution = solutions[name]
    create_classifications(s3, folder, image_name, solution, border, inside, outside)
    if len(border) > third and len(inside) > third and len(outside) > third:
      break
    i += 1


  classifications = border.union(inside).union(outside)
  random.seed(0)
  random.shuffle(classifications)

  index = 0
  while file_id <= total_files and index < len(classifications):
    print("Creating file", file_id)
    key = "classification-{0:d}.classification"
    temp_name = "/tmp/{0:s}".format(key)
    with open(temp_name, "w+") as f:
      items = classifications[index:index + num_items_per_file]
      write_classification(f, items)
      index += len(items)
    s3.Object("maccoss-spacenet", key).put(Body=open(temp_name, "rb"))
    file_id += 1



solutions = parse_solutions("competition1/spacenet_TrainData/vectordata/summarydata/AOI_1_RIO_polygons_solution_3band.csv")
params = json.loads(open("json/spacenet-classification.json").read())
process_images("competition1/spacenet_TrainData/3band", solutions, params)

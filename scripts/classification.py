import boto3
import json
import os
import random
import re
import time
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


def create_classifications(s3, folder, image_name, polygons, border, inside, outside):
  im = plt.imread(folder + "/" + image_name)
  [height, width, dim] = im.shape

  for y in range(height):
    for x in range(width):
      [r, g, b] = im[y, x]
      if len(polygons) == 0:
        outside.add("{r} {g} {b} {c}\n".format(r=r, g=g, b=b, c=OUTSIDE))
      else:
        found = False
        for polygon in polygons:
          if on_border(polygon, x, y):
            border.add("{r} {g} {b} {c}\n".format(r=r, g=g, b=b, c=BORDER))
            found = True
            break
          elif in_polygon(polygon, x, y):
            inside.add("{r} {g} {b} {c}\n".format(r=r, g=g, b=b, c=INSIDE))
            found = True
        if not found:
          outside.add("{r} {g} {b} {c}\n".format(r=r, g=g, b=b, c=OUTSIDE))


def process_images(folder, solutions, params):
  image_names = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
  random.shuffle(image_names)
  image_names = image_names[:100]
  border = set()
  inside = set()
  outside = set()
  s3 = boto3.resource("s3")

  i = 0
  for image_name in image_names:
    print(time.time(), i, image_name)
    name = image_name.replace("3band_", "").replace(".tif", "")
    solution = solutions[name]
    print("Before", "Border", len(border), "Inside", len(inside), "Outside", len(outside))
    create_classifications(s3, folder, image_name, solution, border, inside, outside)
    print("After", "Border", len(border), "Inside", len(inside), "Outside", len(outside))
#    if len(border) > third and len(inside) > third and len(outside) > third:

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
  key = "train.classification"
  temp_name = "/tmp/{0:s}".format(key)
  with open(temp_name, "w+") as f:
    write_classification(f, classifications)
  s3.Object(params["bucket"], key).put(Body=open(temp_name, "rb"))
  print("Final", "Border", len(border), "Inside", len(inside), "Outside", len(outside))


solutions = parse_solutions("../spacenet_data/vector_data/summarydata/AOI_1_RIO_polygons_solution_3band.csv")
params = json.loads(open("../json/spacenet-classification.json").read())
process_images("../spacenet_data/train_data/3band", solutions, params)

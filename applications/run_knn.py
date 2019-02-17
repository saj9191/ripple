import classification
import knn
import math
import subprocess
import util
from iterator import OffsetBounds
from sklearn.neighbors import NearestNeighbors
from database import Database
from typing import List


def __distance__(p, clz):
  [_, _, pr, pg, pb] = p
  [cr, cg, cb, cc] = clz
  return math.sqrt((cr - pr) ** 2 + (cg - pg) ** 2 + (cb - pb) ** 2)


def run(database: Database, test_key: str, params, input_format, output_format, offsets: List[int]):
  train_obj = database.get_entry("maccoss-spacenet", params["train_key"])
  train_it = classification.Iterator(train_obj, OffsetBounds(params["train_offsets"][0], params["train_offsets"][1]))
  train_x = []
  train_y = []
  more = True
  while more:
    [items, _, more] = train_it.next()
    for [r, g, b, c] in items:
      train_x.append([r, g, b])
      train_y.append(c)

  neigh = NearestNeighbors(n_neighbors=params["k"], algorithm="brute")
  neigh.fit(train_x)

  pixels = []
  rgb = []
  with open(test_key) as f:
    lines: List[str] = f.readlines()
    for line in lines:
      [x, y, r, g, b] = list(map(lambda i: int(i), line.split(" ")))
      pixels.append([x, y])
      rgb.append([r, g, b])

  [distances, indices] = neigh.kneighbors(rgb)

  items = []
  for i in range(len(distances)):
    [x, y] = pixels[i]
    neighbors = []
    for j in range(len(distances[i])):
      distance = distances[i][j]
      clz = train_y[indices[i][j]]
      neighbors.append((distance, clz))
    items.append(("{x} {y}".format(x=x, y=y), neighbors))

  output_format["ext"] = "knn"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  with open(output_file, "wb+") as f:
    knn.Iterator.from_array(items, f, {})

  return [output_file]

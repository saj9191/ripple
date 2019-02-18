import knn
import numpy as np
import util
from database import Database
from enum import Enum
from PIL import Image
from typing import List


class Indices(Enum):
  inside = 0
  border = 1
  outside = 2


def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  entries = database.get_entries(params["bucket"], str(params["input_prefix"]) + "/")
  assert(len(entries) == 1)
  entry = entries[0]

  output_format["ext"] = "tiff"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  with open(output_file, "wb+") as f:
    entry.download(f)

  im = Image.open(output_file)
  width, height = im.size

  classifications = np.empty([height, width], dtype=int)
  it = knn.Iterator(database.get_entry(params["bucket"], key.replace("/tmp/", "")))
  more = True
  while more:
    [items, _, more] = it.next()
    # Currently, evaluating using majority rules
    for [point, neighbors] in items:
      [x, y] = list(map(lambda p: int(p), point.split(b' ')))
      scores = [0, 0, 0]
      neighbors = sorted(neighbors, key=lambda n: n[0])
      d1 = neighbors[0][0]
      dk = neighbors[-1][0]
      for i in range(len(neighbors)):
        [d, c] = neighbors[i]
        w = 1 if i == 0 else (dk - d) / (dk - d1)
        scores[c] += w
      m = max(scores)
      top = [i for i, j in enumerate(scores) if j == m]
      if len(top) == 1:
        if top[0] == Indices.border.value:
          im.putpixel((x, y), (255, 0, 0))
        elif top[0] == Indices.inside.value:
          im.putpixel((x, y), (255, 255, 0))
        classifications[y][x] = top[0]
      else:
        classifications[y][x] = Indices.outside.value

  print(classifications)
  print("Saving to output_file", output_file)
  im.save(output_file)

  return [output_file]

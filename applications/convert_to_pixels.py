import util
from database import Database
from PIL import Image
from typing import List


def make_file(output_format, output_files):
  output_format["bin"] += 1
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  util.make_folder(output_format)
  f = open(output_file, "w+")
  output_files.append(output_file)
  return f

def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  im = Image.open(key)
  px = im.load()
  width, height = im.size
  output_format["ext"] = "pixel"

  num_bins = int((width * height + params["pixels_per_bin"] - 1) / params["pixels_per_bin"])
  output_format["bin"] = 0
  output_format["num_bins"] = num_bins
  output_files = []
  f = make_file(output_format, output_files)

  for y in range(height):
    for x in range(width):
      [r, g, b] = px[x, y]
      f.write("{x} {y} {r} {g} {b}\n".format(x=x, y=y, r=r, g=g, b=b))
      if (y * height + x) % params["pixels_per_bin"] == params["pixels_per_bin"] - 1:
        f.close()
        f = make_file(output_format, output_files)
  f.close()

  return output_files

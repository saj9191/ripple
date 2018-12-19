import os

def split_file(folder, file):
  new_folder = "data_counts/" + folder + "/" + file.replace(".csv", "")
  if not os.path.isdir(new_folder):
    os.mkdir(new_folder)
  f = open("data_counts1/" + folder + "/" + file)
  file_to_values = {}
  lines = f.readlines()

  index_to_file = lines[0].strip().split(",")[1:]
  print(index_to_file)
  costs = lines[1].strip().split(",")[1:]
  for index in range(len(costs)):
    key = index_to_file[index]
    if "/" in key:
      parts = index_to_file[index].split("/")[1:]
      if len(parts) > 1:
        subdir = new_folder + "/" + "/".join(parts[:-1])
        if not os.path.isdir(subdir):
          os.mkdir(subdir)
      key = "/".join(index_to_file[index].split("/")[1:])
    file_to_values[key] = {}
    file_to_values[key]["cost"] = costs[index]

  for line in lines[2:]:
    parts = line.strip().split(",")
    specie = parts[0]
    scores = parts[1:]
    for index in range(len(scores)):
      key = index_to_file[index]
      if "/" in key:
        key = "/".join(index_to_file[index].split("/")[1:])
      if len(scores[index]) > 0:
        file_to_values[key][specie] = scores[index]
      else:
        file_to_values[key][specie] = "0"

  for key in file_to_values.keys():
    print("Writing", new_folder, key)
    with open(new_folder + "/" + key, "w+") as f:
      f.write("Cost,{0:s}\n".format(file_to_values[key]["cost"]))
      for specie in file_to_values[key].keys():
        if specie != "cost":
          f.write("{0:s},{1:s}\n".format(specie, file_to_values[key][specie]))


def split_files():
  for root, dirs, files in os.walk("data_counts1"):
    if "/" not in root:
      continue
    [_, folder] = root.split("/")
    for file in files:
      if file == "PXD005709.csv":
        print("folder", folder, "file", file)
        split_file(folder, file)


def main():
  split_files()

if __name__ == "__main__":
  main()

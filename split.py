import boto3

num_entries_per_file = 1*1000*1000
num_entries = 370261379
num_files = (num_entries + num_entries_per_file - 1) / num_entries_per_file

s3 = boto3.resource("s3")
bucket_name = "maccoss-smith-waterman-fasta"


def add(file_id, content):
  key = "uniprot-fasta-{0:d}".format(file_id)
  print("Adding key", key)
  s3.Object(bucket_name, key).put(Body=str.encode(content))


with open("data/uniprot-all.fasta", "r") as f:
  count = 0
  file_id = 1
  content = ""
  for line in f:
    content += line
    count += 1
    if count == num_entries_per_file:
      add(file_id, content)
      file_id += 1
      content = ""
      count = 0

if len(content) > 0:
  add(file_id, content)

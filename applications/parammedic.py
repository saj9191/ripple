import re
import subprocess
import util
from database.database import Database

ITRAQ = re.compile("INFO: iTRAQ: ([0-9]+)-plex reporter ions detected")
SILAC = re.compile("INFO: SILAC: ([0-9]+)Da separation detected.")
PHOSPHORYLATION = re.compile("INFO: Phosphorylation: detected")
TMT6 = re.compile("INFO: TMT: 6-plex reporter ions detected")
TMT10 = re.compile("INFO: TMT: 10-plex reporter ions detected")


def run(database: Database, file: str, params, input_format, output_format):
  database.download(params["database_bucket"], "crux", "/tmp/crux")
  subprocess.call("chmod 755 /tmp/crux", shell=True)

  command = "cd /tmp; ./crux param-medic {0:s}".format(file)
  output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).decode("utf-8")
  print(output)

  phos = PHOSPHORYLATION.search(output)
  itraq = ITRAQ.search(output)
  silac = SILAC.search(output)
  tmt6 = TMT6.search(output)
  tmt10 = TMT10.search(output)

  map_bucket = None
  if tmt6:
    if phos:
      map_bucket = "maccoss-tmt6-phosphorylation-fasta"
    else:
      map_bucket = "maccoss-tmt6-fasta"
  elif tmt10:
    if phos:
      map_bucket = "maccoss-tmt10-phosphorylation-fasta"
    else:
      map_bucket = "maccoss-tmt10-fasta"
  elif itraq:
    if phos:
      map_bucket = "maccoss-itraq-phosphorylation-fasta"
    else:
      map_bucket = "maccoss-itraq-fasta"
  elif phos:
    map_bucket = "maccoss-phosphorylation-fasta"
  elif silac:
    map_bucket = "maccoss-silac-fasta"
  else:
    map_bucket = "maccoss-normal-fasta"

  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["bucket"],
        },
        "object": {
          "key": util.file_name(input_format),
        },
        "extra_params": {
          "map_bucket": map_bucket,
          "prefix": output_format["prefix"],
        }
      }
    }]
  }

  output_file = util.file_name(output_format)
  database.write(params["bucket"], output_file, output, {}, False)
  database.invoke(params["output_function"], payload)

  return []

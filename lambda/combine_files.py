import importlib
import os
import util


def combine(bucket_name, key, input_format, output_format, offsets, params):
  util.print_read(input_format, key, params)

  batch_size = params["batch_size"] if "batch_size" in params else 1
  start_file_id = int((input_format["file_id"] + batch_size - 1) / batch_size)

  output_format["file_id"] = start_file_id
  output_format["bin"] = 1
  util.make_folder(output_format)

  [combine, batches] = util.combine_instance(bucket_name, key, params)
  if combine:
    for i in range(len(batches)):
      keys = batches[i]
      output_format["file_id"] = start_file_id + i
      output_format["last"] = (i + 1) == len(batches)
      msg = "Combining TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d}"
      msg = msg.format(input_format["timestamp"], input_format["nonce"], input_format["bin"], input_format["file_id"])
      print(msg)

      format_lib = importlib.import_module(params["format"])
      iterator = getattr(format_lib, "Iterator")
      file_name = util.file_name(output_format)
      temp_name = "/tmp/{0:s}".format(file_name)
      # Make this deterministic and combine in the same order
      keys.sort()
      iterator.combine(bucket_name, keys, temp_name, params)
      f = open(temp_name, "rb")
      util.write(input_format, params["bucket"], file_name, f, params)
      f.close()
      os.remove(temp_name)


def handler(event, context):
  [bucket_name, key, params] = util.lambda_setup(event, context)
  m = util.run(bucket_name, key, params, combine)
  util.show_duration(context, m, params)

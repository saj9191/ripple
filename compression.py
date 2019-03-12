import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 2240,
}
pipeline = ripple.Pipeline(name="compression", table="s3://maccoss-methyl", log="s3://maccoss-methyl-log", timeout=600, config=config)
input = pipeline.input(format="new_line")
#step = input.split({"split_size": 110*1000*1000}, {"memory_size": 128})
step = input.split({"split_size": 500*1000*1000}, {"memory_size": 128})
step = step.run("compress_methyl", params={"program_bucket": "maccoss-methyl-data"})
pipeline.compile("json/compression.json")

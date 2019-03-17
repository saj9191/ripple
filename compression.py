import ripple

config = {
  "region": "us-west-1",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 2240,
}
pipeline = ripple.Pipeline(name="compression", table="s3://maccoss-methyl-west-1", log="s3://maccoss-log-west-1", timeout=600, config=config)
input = pipeline.input(format="bed")
step = input.sort(identifier="start_position", params={"split_size": 500*1000*1000}, config={"memory_size": 3008})
step = step.run("compress_methyl", params={"program_bucket": "ssw-database-west-1"})
pipeline.compile("json/compression.json")

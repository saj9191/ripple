import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008,
}
pipeline = ripple.Pipeline(name="compression", table="s3://maccoss-tide-west-2", log="s3://maccoss-log-west-2", timeout=600, config=config)
input = pipeline.input(format="bed")
step = input.sort(identifier="start_position", params={"split_size": 500*1000*1000, "num_bins": 35}, config={"memory_size": 3008})
step = step.run("compress_methyl", params={"program_bucket": "maccoss-methyl-data"})
pipeline.compile("json/compression.json")

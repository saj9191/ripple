import ripple

config = {
  "region": "us-east-1",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008,
}
pipeline = ripple.Pipeline(name="compression", table="s3://maccoss-tide-east-1", log="s3://maccoss-log-east-1", timeout=600, config=config)
input = pipeline.input(format="bed")
step = input.sort(identifier="start_position", params={"split_size": 500*1000*1000}, config={"memory_size": 3008})
step = step.run("compress_methyl", params={"program_bucket": "maccoss-fasta-east-1"})
pipeline.compile("json/compression.json", dry_run=True)

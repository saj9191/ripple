import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008
}
pipeline = ripple.Pipeline(name="tide", table="s3://maccoss-tide", log="s3://maccoss-log", timeout=600, config=config)
input = pipeline.input(format="mzML")
step = input.top(identifier="tic", number=1000)

params={
  "database_bucket": "maccoss-fasta"
}
step = input.run("parammedic", params=params, output_format="mzML")
pipeline.compile("json/ppm-tide.json", dry_run=False)

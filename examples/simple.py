import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 1024,
}
pipeline = ripple.Pipeline(name="tide", table="s3://maccoss-tide", log="s3://maccoss-log", timeout=600, config=config)
input = pipeline.input(format="new_line")
step = input.run("echo")

pipeline.compile("json/simple.json", dry_run=False)

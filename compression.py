import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008
}
pipeline = ripple.Pipeline(name="compression", table="s3://maccoss-methyl", log="s3://maccoss-methyl-log", timeout=600, config=config)
input = pipeline.input(format="new_line")
step = input.split({"split_size": 100000000}, {"memory_size": 128})
step = step.run("compress_methyl", params={"program_bucket": "ripple-program"}, config={"memory_size": 1024})
pipeline.compile("json/compile.json", dry_run=True)

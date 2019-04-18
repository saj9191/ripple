import ripple

config = {
  "region": "us-east-1",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008,
}
pipeline = ripple.Pipeline(name="document", table="s3://maccoss-tide-east-1", log="s3://maccoss-log-east-1", timeout=600, config=config)
input = pipeline.input(format="new_line")
input.map(table="train-data", func=lambda input_key, bucket_key: input_key.run("compress_fastore", params={"train_data": bucket_key}))
pipeline.compile("json/document.json", dry_run=True)

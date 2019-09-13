import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008
}
pipeline = ripple.Pipeline(name="tide", table="s3://maccoss-tide", log="s3://maccoss-log", timeout=600, config=config)
params={
  "database_bucket": "maccoss-fasta"
}

input = pipeline.input(format="mzML")
# Filter for the most intense spectra
step = input.top(identifier="tic", number=1000)
# Run param-medic on top spectra
step = input.run("parammedic", params=params, output_format="mzML")
# For each matching FASTA, run Tide
tide = lambda input_key, bucket_key: input_key.run("tide", params={
  "database_bucket": "maccoss-fasta",
  "num_threads": 0,
  "output_format": "tsv",
  "species": bucket_key,
})
step = step.map(func=tide, params={"directories": True})
# Calculate the confidence score of the results
step = step.run("confidence", params=params, output_format="confidence")
# Find the result with the top confidence score 
step = step.match("qvalue")

pipeline.compile("json/medic-tide.json", dry_run=False)

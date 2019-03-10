import ripple

config = {
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole",
  "memory_size": 3008
}
pipeline = ripple.Pipeline(name="spacenet", table="s3://maccoss-methyl", log="s3://maccoss-methyl-log", timeout=600, config=config)
input = pipeline.input(format="tif")
step = input.run("convert_to_pixels", params={"pixels_per_bin": 1000}, output_format="pixel", config={"memory_size": 128})
step = step.run("pair", params={"split_size": 10*1000*1000}, config={"memory_size": 128})
step = step.run("run_knn", {"k": 100}, output_format="knn")
step = step.combine(params={"k": 100, "sort": True})
step = step.combine(params={"k": 100,  "sort": False})
step = step.run("draw_borders", {"image": input}, output_format="tif", config={"memory_size": 1024})
pipeline.compile("json/spacenet-classification.json", dry_run=True)

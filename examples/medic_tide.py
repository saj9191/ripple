import argparse
import boto3
import ripple
import time
import upload


def setup_ripple(num_top):
    config = {
        "region": "us-west-2",
        "role": "service-role/lambdaFullAccessRole",
        "memory_size": 3008
        }
    pipeline = ripple.Pipeline(name="tide", table="s3://maccoss-tide", log="s3://maccoss-log", timeout=600,
                               config=config)
    params = {
        "database_bucket": "maccoss-fasta"
        }

    input = pipeline.input(format="mzML")
    # Filter for the most intense spectra
    step = input.top(identifier="tic", number=num_top)
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


def get_result(key):
    parts = key.split("/")
    prefix = "6/" + parts[1]
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("maccoss-tide")
    objects = []
    while len(objects) == 0:
        objects = list(bucket.objects.filter(Prefix=prefix))
        time.sleep(5)

    assert (len(objects) == 1)
    match = objects[0].get()["Body"].read().decode("utf-8")
    print("")
    print("**********************************************")
    print("Best match", match.split("-")[-1].split(".")[0])
    print("**********************************************")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_top", type=int, required=True, help="Number of top spectra to use")
    parser.add_argument("--key", type=str, required=True, help="Name of file to identify")
    args = parser.parse_args()

    setup_ripple(args.num_top)
    key, _, _ = upload.upload("maccoss-tide", args.key, "tide-source-data")
    get_result(key)


main()

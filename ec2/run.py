import argparse
import json
import master


def setup(s3_application_url, bucket, max_nodes, params):
  # The master node is responsible for monitoring the health of data nodes
  # and maintains a queue of pending tasks. Since the goal is to emulate
  # EMR, we will not include the master node in the costs.
  m = master.Master(bucket, max_nodes, s3_application_url, params)
  m.setup()
  return m


def run(application_folder, bucket, max_nodes, params):
  m = setup(application_folder, bucket, max_nodes, params)
  m.run()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--s3_application_url",
                      type=str,
                      required=True,
                      help="S3 folder containing all scripts needed for application. Should include main.py."
                      )
  parser.add_argument("--bucket", type=str, required=True, help="Bucket to store input / output")
  parser.add_argument("--max_nodes", type=int, default=10, help="Maximum number of nodes in cluster")
  parser.add_argument("--parameters", type=str, required=True, help="Parameters such as security group for nodes")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  run(args.s3_application_url, args.bucket, args.max_nodes, params)


if __name__ == "__main__":
  main()

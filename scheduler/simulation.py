import argparse
import boto3
import inspect
import json
import os
import scheduler
import sys
import time
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import setup


def create_jobs(policy):
  source_bucket = "tide-source-data"
  destination_bucket = "maccoss-tide"
  prefix = "DIA-Files/"
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(source_bucket)
  objs = list(bucket.objects.filter(Prefix=prefix))
  objs = list(filter(lambda obj: obj.key.endswith("mzML"), objs))
  job_duration = 180

  if policy == "deadline":
    jobs = []
    offset = 60
    now = time.time() + 10
    num_jobs = 10
    for i in range(num_jobs):
      obj = objs[i]
      jobs.append(scheduler.Job(source_bucket, destination_bucket, obj.key, start_time=now + i * offset, deadline=now + job_duration + (2*(num_jobs-1)- i) * offset))
  elif policy == "priority":
    raise Exception("Not implemented")
  jobs = scheduler.simulation_order(jobs, policy, job_duration, 1)
  jobs = list(map(lambda j: j[0], jobs))
  for job in jobs:
    print(job)
  return jobs


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--policy", type=str, default="fifo", help="Scheduling policy to use (fifo, robin, deadline)")
  parser.add_argument("--timeout", type=int, default=60, help="How long we should wait for a task to retrigger")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  setup.process_functions(params)
  jobs = create_jobs(args.policy)
  s = scheduler.Scheduler(args.policy, args.timeout, params)
  s.add_jobs(jobs)
  s.listen()


main()

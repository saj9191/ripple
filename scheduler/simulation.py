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


def create_jobs(source_bucket, destination_bucket, policy, prefix, num_jobs, job_duration, offset, relative):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(source_bucket)
  objs = list(bucket.objects.filter(Prefix=prefix))
  objs = list(filter(lambda obj: not obj.key.endswith("/"), objs))
  now = 0 if relative else time.time() + 10

  if policy == "deadline":
    jobs = []
    for i in range(num_jobs):
      obj = objs[i]
      start_time = now + i * offset
      deadline = now + job_duration + (2 * (num_jobs - 1) - i) * offset
      jobs.append(scheduler.Job(source_bucket, destination_bucket, obj.key, start_time=start_time, deadline=deadline))
  elif policy == "priority":
    jobs = []
    for i in range(num_jobs):
      obj = objs[i]
      start_time = now + i * offset
      if i % 3 == 1:
        priority = 10
      else:
        priority = 1
      jobs.append(scheduler.Job(source_bucket, destination_bucket, obj.key, start_time=start_time, priority=priority))

  jobs = scheduler.simulation_order(jobs, policy, job_duration, 1)
  jobs = list(map(lambda j: j[0], jobs))
  for job in jobs:
    print(job)
  return jobs


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--parameters", type=str, required=True, help="File containing parameters")
  parser.add_argument("--run", action="store_true", default=False, help="Run simulation")
  parser.add_argument("--relative", action="store_true", default=False, help="Store timestamps assuming first timestamp is at time 0")
  args = parser.parse_args()

  p = json.loads(open(args.parameters).read())
  params = json.loads(open(p["parameters"]).read())
  setup.process_functions(params)
  jobs = create_jobs(p["source_bucket"], params["bucket"], p["policy"], p["prefix"], p["num_jobs"], p["duration"], p["offset"], args.relative)
  if args.run:
    s = scheduler.Scheduler(p["policy"], p["timeout"], params)
    s.add_jobs(jobs)
    s.listen(p["num_invokers"], p["num_loggers"])


main()

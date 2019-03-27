import boto3
from dateutil import parser
import json
import matplotlib
import os
import setup
import statistics
import sys
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec


DATE_INDEX = 6


def chorus_timestamps(file_name):
  dates = []
  f = open(file_name)
  lines = f.readlines()[1:]
  for line in lines:
    dt = line.split(",")[DATE_INDEX]
    dt = dt[1:-2]
    parts = dt.split(" ")
    if len(parts) != 2:
      continue
    date = parser.parse(dt)
    s = "{0:d}-{1:02d}-{2:02d}".format(date.year, date.month, date.day)
    if "2017-09-25" <= s and s <= "2017-10-21":
      dates.append(date)

  first_date = dates[0]
  timestamps = list(map(lambda d: (d - first_date).total_seconds(), dates))
  return timestamps


def ripple_timestamps(bucket_name, log_bucket_name, params):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  objs = bucket.objects.filter(Prefix="0/")
  timestamps = []
  i = 0
  keys = []
  for obj in objs:
    token = obj.key.split("/")[1]
    keys.append(obj.key)
    ts = float(token.split("-")[0])
    if i >= 574:
      ts -= 15565
    timestamps.append(ts)
    output_file = "nature_results/simulation/{0:s}".format(token)
    if not os.path.isfile(output_file):
      open(output_file, "a").close()
      print("Recording", token)

      statistics.statistics(log_bucket_name, token, None, params, output_file)
    i += 1
  first_timestamp = timestamps[0]
  timestamps = list(map(lambda ts: ts - first_timestamp, timestamps))
  return [timestamps, keys]


def compare(rts, cts):
  for i in range(len(rts)):
    print(i, "ripple", rts[i], "chorus", cts[i])


def main1():
  params = json.loads(open("json/private-ppm-east-2.json").read())
  setup.process_functions(params)
  rts = ripple_timestamps("maccoss-tide-east-2", "maccoss-log-east-2", params)
  cts = chorus_timestamps("ChorusAccessLog.csv")
  compare(rts, cts)


def parse_file(file, timestamp, first_timestamp, offset):
  content = open(file).read()
  statistics = json.loads(content)["stats"]
  points = []
  upload_finish_timestamp = None
  for stage in range(len(statistics)):
    messages = statistics[stage]["messages"]
    for message in messages:
      start_time = message["start_time"] - first_timestamp - offset
      if stage == 0:
        upload_finish_timestamp = start_time
      end_time = start_time + message["duration"] / 1000.0
      points.append([start_time, 1, stage])
      points.append([end_time, -1, stage])

  upload_timestamps = [timestamp - first_timestamp, upload_finish_timestamp]
  return [points, upload_timestamps]


def calculate_counts(stage_to_timestamps):
  stage_to_counts = {}
  for stage in stage_to_timestamps.keys():
    if stage not in stage_to_counts:
      stage_to_counts[stage] = []


def parse_timestamps(rts, keys):
  points = []
  upload_points = []
  first_timestamp = rts[0]

  offset = 0
  for i in range(len(keys)):
    timestamp = rts[i]
    file = keys[i].split("/")[1]
    if i == 574:
      offset = 15565
    timestamp -= offset
    [file_points, file_upload_timestamps] = parse_file("nature_results/simulation/" + file, timestamp, first_timestamp, offset)
    points += file_points
    upload_points += file_upload_timestamps
  points.sort()
  return [points, upload_points]


def calculate_stage_counts(points):
  stage_to_timestamps = {}
  stage_to_counts = {}
  for [time, increment, stage] in points:
    if stage not in stage_to_counts:
      stage_to_counts[stage] = [0]
      stage_to_timestamps[stage] = [0]

    count = stage_to_counts[stage][-1]
    count += increment
    stage_to_counts[stage].append(count)
    stage_to_timestamps[stage].append(time)
  return [stage_to_timestamps, stage_to_counts]


def timestamp_label(timestamp, tick_number):
  timestamp = int(timestamp / 60)
  minutes = timestamp % 60
  timestamp = int(timestamp / 60)
  hours = timestamp % 24
  day = int(timestamp / 24)
  return "01/{0:02d} {1:02d}:{2:02d}".format(day + 1, hours, minutes)


def plot(stage_to_timestamps, stage_to_counts, points, chorus_ts, pipeline, plot_name, folder, zoom):
  plt.subplots_adjust(hspace=0, wspace=0)
  font_size = 16

  fig1 = plt.figure()
  gs1 = gridspec.GridSpec(2, 1)
  zoom_axis = plt.subplot(gs1[0])
  zoom_axis.locator_params(axis='x', nbins=5)
  ripple_axis = plt.subplot(gs1[1])
  ripple_axis.locator_params(axis='x', nbins=5)
  colors = ["blue", "cyan", "orange", "purple", "magenta", "black"]
  legends = []

  for axis in [zoom_axis, ripple_axis]:
    axis.margins(x=0, y=0)
    for side in ["right", "top"]:
      axis.spines[side].set_visible(False)

    for side in ["left", "bottom"]:
      axis.spines[side].set_linewidth(3)

  for stage in range(len(pipeline) + 1):
    color = colors[stage % len(colors)]
    label = pipeline[stage]["name"] if stage < len(pipeline) else "Total"
    patch = mpatches.Patch(facecolor=color, edgecolor="black", label=label, linewidth=1, linestyle="solid")
    legends.append(patch)

  regions = {}
  max_y = 0
  max_x = 0
  increment = 5
  for stage in stage_to_timestamps:
    min_x = sys.maxsize
    px = []
    py = []
    color = colors[stage % len(colors)]
    for i in range(len(stage_to_timestamps[stage])):
      lx = stage_to_timestamps[stage][i]
      ly = stage_to_counts[stage][i]
      if len(py) > 0 and lx - px[-1] > increment:
        r = range(int(px[-1]) + increment, int(lx), increment)
        for dx in r:
          px.append(dx)
          py.append(py[-1])
      if ly > 0:
        px.append(lx)
        py.append(ly)

    if len(py) > 0:
      max_y = max(max_y, max(py))
      min_x = min(min_x, min(px))
      max_x = max(max_x, max(px))

    regions[stage] = [min_x, max_x]
    zoom_axis.plot(px, py, color=color)
    ripple_axis.plot(px, py, color=color)

  chorus_y_points = list(map(lambda x: max_y, chorus_ts))
  for axis in [ripple_axis, zoom_axis]:
    axis.scatter(chorus_ts, chorus_y_points, color="red", s=1)

  zoom_axis.set_xlim(zoom)
  zoom_axis.xaxis.set_major_formatter(plt.FuncFormatter(timestamp_label))
  ripple_axis.set_xlim([0, max_x])
  ripple_axis.xaxis.set_major_formatter(plt.FuncFormatter(timestamp_label))

  plot_name = "{0:s}/{1:s}.png".format(folder, plot_name)
  plt.xlabel("Runtime (seconds)", size=font_size)
  print(plot_name)
  gs1.tight_layout(fig1)
  fig1.savefig(plot_name)
  print("Accumulation plot", plot_name)
  plt.close()


def main():
  chorus_ts = chorus_timestamps("ChorusAccessLog.csv")
  params = json.loads(open("json/private-ppm-east-2.json").read())
  [rts, keys] = ripple_timestamps("maccoss-tide-east-2", "maccoss-log-east-2", params)
  setup.process_functions(params)
  [ripple_points, upload_points] = parse_timestamps(rts, keys)

  [stage_to_timestamps, stage_to_counts] = calculate_stage_counts(ripple_points)
  zoom = [11000, 14600]
  plot(stage_to_timestamps, stage_to_counts, ripple_points, chorus_ts, params["pipeline"], "accumulation", "nature_results", zoom)


if __name__ == "__main__":
  main()

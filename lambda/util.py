import subprocess

def clear_tmp():
  subprocess.call("rm -rf /tmp/*", shell=True)

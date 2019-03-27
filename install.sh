#!/bin/bash
sudo python3.6 -m pip install --upgrade pip
sudo python3.6 -m pip install -U mypy
sudo python3.6 -m pip install boto3

cmd="export PYTHONPATH=\$PYTHONPATH:$PWD"
echo $cmd >> ~/.bashrc

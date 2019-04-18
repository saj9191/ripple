#!/bin/bash
sudo python -m pip install --upgrade pip
sudo python -m pip install -U mypy
sudo python -m pip install boto3

cmd="export PYTHONPATH=\$PYTHONPATH:$PWD"
echo $cmd >> ~/.bashrc

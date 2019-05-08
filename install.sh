#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run script as root"
	exit
fi

version=`python -c 'import sys; print(sys.version_info[:][0])'`
if [ "$version" -lt 3 ]
  then echo "Please upgrade to Python 3"
  exit
fi

python -m pip install --upgrade pip
python -m pip install -U mypy
python -m pip install boto3

cmd="export PYTHONPATH=\$PYTHONPATH:$PWD:$PWD/applications:$PWD/lambda:$PWD/formats"
echo $cmd >> ~/.bashrc

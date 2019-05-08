#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run script as root"
	exit
fi

version=`python -c 'import sys; print(sys.version_info[:][0:2])'`
if [ ${version:1:1} -lt 3 ]
  then
  echo "Please upgrade to Python 3.6"
  exit
elif [ ${version:1:1} -eq 3 ]
  then
  if [ ${version:4:1} -lt 6 ]
    then
    echo "Please upgrade to Python 3.6"
    exit
  fi
fi

python -m pip --version
RESULT=$?
if [ $RESULT -ne 0  ]
  then echo "Please install pip"
  exit
fi

python -m pip install --upgrade pip
python -m pip install -U mypy
python -m pip install boto3
python -m pip install numpy
python -m pip install Pillow

cmd="export PYTHONPATH=\$PYTHONPATH:$PWD:$PWD/applications:$PWD/lambda:$PWD/formats"
echo $cmd >> ~/.bashrc

#!/bin/bash

export PYTHONPATH="$PYTHONPATH:$PWD/formats"

if [ "$1" == "lambda" ]; then
  export PYTHONPATH="$PYTHONPATH:$PWD/lambda"
  if [ ${#2} == 0 ]; then
    pattern="lambda_*_test.py"
  else
    pattern="lambda_${2}_test.py"
  fi
elif [ "$1" == "applications" ]; then
  export PYTHONPATH="$PYTHONPATH:$PWD/applications"
  export PYTHONPATH="$PYTHONPATH:$PWD/lambda"
  if [ ${#2} == 0 ]; then
    pattern="applications_*_test.py"
  else
    pattern="applications_${2}_test.py"
  fi
elif [ "$1" == "format" ]; then
	if [ ${#2} == 0 ]; then
    pattern="format_*_test.py"
	else
    pattern="format_${2}_test.py"
	fi
elif [ "$1" == "pipeline" ]; then
  export PYTHONPATH="$PYTHONPATH:$PWD/applications"
  export PYTHONPATH="$PYTHONPATH:$PWD/lambda"
	if [ ${#2} == 0 ]; then
    pattern="pipeline_*_test.py"
	else
    pattern="pipeline_${2}_test.py"
	fi
elif [ "$1" == "unit" ]; then
  export PYTHONPATH="$PYTHONPATH:$PWD/applications"
  export PYTHONPATH="$PYTHONPATH:$PWD/lambda"
  pattern="lambda_*_test.py"
  python -m unittest discover -s tests -p $pattern
  pattern="format_*_test.py"
  python -m unittest discover -s tests -p $pattern
  pattern="applications_*_test.py"
elif [ ${#1} == 0 ]; then
	pattern="*_test.py"
fi

python -m unittest discover -s tests -p $pattern

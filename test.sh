#!/bin/bash

if [ "$1" == "lambda" ]; then
  if [ ${#2} == 0 ]; then
    pattern="lambdas_*_test.py"
  else
    pattern="lambdas_${2}_test.py"
  fi
elif [ "$1" == "applications" ]; then
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
  if [ ${#2} == 0 ]; then
    pattern="pipeline_*_test.py"
  else
    pattern="pipeline_${2}_test.py"
  fi
elif [ "$1" == "unit" ]; then
  pattern="lambdas_*_test.py"
  python -m unittest discover -s tests -p $pattern
  pattern="format_*_test.py"
  python -m unittest discover -s tests -p $pattern
  pattern="applications_*_test.py"
elif [ ${#1} == 0 ]; then
  pattern="*_test.py"
fi

python -m unittest discover -s tests -p $pattern

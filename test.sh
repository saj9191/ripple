#!/bin/bash

if [ "$1" == "format" ]; then
  pattern="format_*_test.py"
else
	pattern="*_test.py"
fi
python3.6 -m unittest discover -s tests -p $pattern

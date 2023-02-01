#!/bin/sh
set -e

flake8 --config=flake8.cfg
pypy -m unittest discover
#!/usr/bin/env bash
unset PYTHONPATH
python3 -m venv env
echo "unset PYTHONPATH" >> env/bin/activate
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt -r requirements-dev.txt

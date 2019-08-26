#!/bin/bash
while ! nc -z database 3306; do sleep 3; done
python api.py

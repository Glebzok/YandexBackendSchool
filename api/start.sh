#!/bin/bash
while ! nc -z database 3306; do sleep 3; done
gunicorn -b 0.0.0.0:8080 --workers=9 --worker-class=gevent --worker-connections=100 api:app

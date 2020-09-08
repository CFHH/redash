#!/bin/bash

source /root/vredash/bin/activate
cd /root/vredash/redash

redis-cli -h localhost -p 6379 flushall
nohup ./manage.py runserver  --host 0.0.0.0 &
nohup ./manage.py rq scheduler &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker scheduled_queries periodic emails default schemas &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &
nohup ./manage.py rq worker queries &

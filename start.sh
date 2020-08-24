#!/bin/bash
redis-cli -h localhost -p 6379 flushall
./manage.py runserver  --host 0.0.0.0 &
./manage.py rq scheduler &
./manage.py rq worker queries &
./manage.py rq worker scheduled_queries periodic emails default schemas &

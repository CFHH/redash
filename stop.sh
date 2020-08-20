#!/bin/bash
kill -9 $(ps -ef|grep manage.py|grep -v grep|awk '{print $2}')

#!/bin/sh

set -e

LISTEN_PID=$$
export LISTEN_PID

env | grep LISTEN

exec /home/kr/bin/beanstalkd

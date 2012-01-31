#!/bin/sh
set -e

if [ ! -x make.sh ]; then
	echo 'all.sh must be run from the root of the doozerd source tree.' 1>&2
	exit 1
fi

sh make.sh
go test ./...

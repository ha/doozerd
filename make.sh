#!/bin/sh
set -e

if [ ! -x all.sh ]; then
	echo 'make.sh must be run from the root of the doozerd source tree.' 1>&2
	exit 1
fi

PKGS="
	web
	consensus
	peer
	server
"

for pkg in $PKGS
do
	make -C $pkg install
done

go install

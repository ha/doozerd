#!/bin/sh
set -e

if [ -f env.sh ]
then . ./env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

mtest() {
    cd pkg/$1
    gomake test
}

(mtest)
for pkg in $PKGS
do (mtest $pkg)
done

#!/bin/sh
set -e

if [ -f env.sh ]
then . ./env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

args="$@"

mtest() {
    echo
    echo --- test $1
    cd pkg/$1
    gotest $args
}

(mtest)
for pkg in $PKGS
do (mtest $pkg)
done

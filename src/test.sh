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
    name=$(echo $1 | sed 's/\//_/')
    cat <<TEST
it_passes_$name() {
    cd $1
    gotest $args
}
TEST
}

{
    mtest pkg
    for pkg in $PKGS
    do mtest pkg/$pkg
    done
} > all-test.sh

roundup

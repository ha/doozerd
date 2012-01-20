#!/bin/bash
set -e
eval `gomake -f Make.inc go-env`
./clean.sh
./all.sh
base=`./cmd/doozerd/doozerd -v|tr ' ' -`
file=$base-$GOOS-$GOARCH.tar
trap "rm -rf $base $file" 0
mkdir $base
cp cmd/doozerd/doozerd $base
cp ../README.md $base/README.md
tar cf $file $base
gzip -9 $file

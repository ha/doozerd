#!/bin/sh
set -e
if [ -f env.sh ]
then . ./env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

xcd() {
    echo
    cd $1
    echo --- cd $1
}

mk() {
    d=$PWD
    xcd $1
    gomake clean
    cd "$d"
}

rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer
rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer.a
rm -rf $GOBIN/doozerd

# remove auto generated protobuffer files
find . -name "*.pb.go" -exec rm {} \;

for pkg in $PKGS
do mk pkg/$pkg
done

for cmd in $CMDS
do mk cmd/$cmd
done

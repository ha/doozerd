#!/bin/sh
set -e

if [ -f env.sh ]
then . env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

xcd() {
    echo
    cd $1
    echo --- cd $(pwd)
}

mk() {
    xcd $1
    gomake clean
    gomake
    gomake install
}

rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer
rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer.a
rm -rf $GOBIN/doozerd

for req in $REQS
do goinstall $req
done

for pkg in $PKGS
do (mk pkg/$pkg)
done

(mk pkg)

sh test.sh

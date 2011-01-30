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
    xcd $1
    gomake clean
    gomake
    gomake install
}

rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer
rm -rf $GOROOT/pkg/${GOOS}_${GOARCH}/doozer.a
rm -rf $GOBIN/doozerd

export VERSION
VERSION=`git describe | sed s/^v// | tr - +`
if ! git diff --quiet HEAD
then
    VERSION=$VERSION+mod
fi

for req in $PKG_REQS
do goinstall $req
done

for p in $CMD_REQS
do (mk $p)
done

for pkg in $PKGS
do (mk pkg/$pkg)
done

for cmd in $CMDS
do (mk cmd/$cmd)
done

echo
echo "--- TESTING"

/bin/sh test.sh

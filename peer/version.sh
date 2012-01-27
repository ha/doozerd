#!/bin/sh

git describe | sed s/^v// | tr - + | tr -d '\n'
if ! git diff --quiet HEAD
then echo -n +mod
fi
echo

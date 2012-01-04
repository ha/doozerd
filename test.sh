#!/bin/sh
set -e
dirs=$(find . -iname "*.go" | xargs -n1 dirname | sort | uniq | grep -v '\.$')
go test $dirs

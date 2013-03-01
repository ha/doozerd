#!/bin/sh
set -e
sh make.sh
go test -v ./...

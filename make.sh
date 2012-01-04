#!/bin/sh
set -e

PKG=web

GOFILES="
	main.css.go
	main.html.go
	stats.html.go
	main.js.go
"

for f in $GOFILES
do 
  b="web/$(basename $f .go)"
  ./web/file2gostring $PKG $b < $b > web/$f.part
  mv web/$f.part web/$f
done

ver=$(./version.sh)
printf "package peer;const Version = \`%s\`\n" "$ver" > peer/version.go
go install

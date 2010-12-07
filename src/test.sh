#!/bin/sh
set -e

if ! which roundup > /dev/null
then
    echo "You need to install roundup to run this script."
    echo "See: http://bmizerany.github.com/roundup"
    exit 1
fi

if [ -f env.sh ]
then . ./env.sh
else
    echo 1>&2 "! $0 must be run from the root directory"
    exit 1
fi

{
    for pkg in $PKGS
    do
        name=$(echo $pkg | sed 's/\//_/' | tr -d .)
        echo "it_passes_pkg_$name() { cd pkg/$name; gotest $@; }"
    done
} > all-test.sh

roundup

rm all-test.sh

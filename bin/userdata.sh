set -x

## Prereqsuisites
apt-get update

apt-get install -y\
    build-essential\
    bison\
    git-core\
    mercurial

## Workarea
cd /opt/

[ ! -d go ] && {
    hg clone -r release https://go.googlecode.com/hg/ go
}
cd go/src

GOBIN="/usr/local/bin"
export GOBIN

./all.bash

cd /opt
## Roundup
[ ! -d roundup ] && {
    git clone http://github.com/bmizerany/roundup.git
}
cd roundup
./configure
make install

cd /opt
## Doozer
[ ! -d doozer ] && {
    git clone http://github.com/bmizerany/doozer.git
}

cd doozer/src
./all.sh

# Doozer

## Installing Go

I recommend not using apt, homebrew, or any other packaging system to install
Go. It's better to install straight from source.

Abridged version:

    $ GOROOT=$HOME/src/go # adjust this as you wish
    $ hg clone -r release https://go.googlecode.com/hg/ $GOROOT
    $ cd $GOROOT/src
    $ ./all.bash
    # put $GOROOT/bin in your path

For full details, see <http://golang.org/doc/install.html>.

## Installing Dependencies

If you want to run doozer's tests, install
<http://bmizerany.github.com/roundup/>.

## Installing Doozer

    $ git clone git@github.com:bmizerany/doozer.git
    $ cd doozer/src
    $ ./all.sh

## Try It Out

    $ bin/strap

This will start up three doozer processes and open a web view on one of them.

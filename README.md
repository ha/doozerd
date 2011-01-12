# Doozer

## Installing Go

I recommend not using apt, homebrew, or any other packaging system to install
Go. It's better to install straight from source.

Abridged version:

    $ GOROOT=$HOME/src/go # adjust this as you wish
    $ hg clone -r release https://go.googlecode.com/hg/ $GOROOT
    $ cd $GOROOT/src
    $ ./all.bash
    # put the value of $GOROOT/bin in your path

For full details, see <http://golang.org/doc/install.html>.

## Installing Dependencies

If you want to run doozer's tests, install
<http://bmizerany.github.com/roundup/>.

## Installing Doozer

    $ git clone https://github.com/bmizerany/doozer.git
    $ cd doozer/src
    $ ./all.sh

## Try It Out

    $ doozerd
    $ open http://localhost:8080/

This will start up one doozer process and show a web view of its contents.

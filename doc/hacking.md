# Hacking on Doozer

If you want to hack on doozer, we suggest discussing your plans on the
[mailing list][mail] to avoid duplicating effort.
But if not, that's cool too.  Just have fun.

Here are some instructions for building doozer from source:

## Installing Go

I recommend not using apt, homebrew, or any other packaging system to install
Go. It's better to install straight from the official Go packages.
Easy-to-follow instructions are at <http://golang.org/doc/install>.

## Installing Dependencies

If you want to change .proto files, you need to nstall the `protoc`
command (from <http://github.com/golang/protobuf/>):

    $ sudo apt-get install protobuf-compiler
    (or)
    $ brew install protobuf

If you want to run doozer's tests, install
<http://bmizerany.github.com/roundup/>.

## Building Doozer

    (make sure you have set $GOPATH)
    $ mkdir -p $GOPATH/src/github.com/ha/
    $ git clone https://github.com/ha/doozerd.git
    $ cd doozerd
    $ ./all.sh

This will build the rest of the dependencies and
all doozer packages and commands,
and copy the commands into `$GOPATH/bin`. You can test individual doozer
components by running `go test` in that sub-package directory.

## Try It Out

    $ doozerd >/dev/null 2>&1 &
    $ open http://localhost:8000/

This will start up one doozer process and show a web view of its contents.

[mail]: https://groups.google.com/group/doozer

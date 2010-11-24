# Doozer

# A word about OS X

Doozer has deep integration with the /proc file system
and takes advantage of Unix features not available on OS X.

There are some "hacks" in place to allow you to do everything
except process managment on OS X.

If you use Windows, you're fired.

### Installing Go

http://golang.org/doc/install.html

### Installing Doozer

    $ git clone git@github.com:bmizerany/doozer.git
    $ cd doozer
    $ make


You can test it works with:

    $ doozerd

### Run Tests

    $ goinstall github.com/bmizerany/assert
    $ make test

Point your browser at http://127.0.0.1:8080

# Junta

# A word about OS X

Junta has deep integration with the /proc file system
and takes advantage of Unix features not available on OS X.

There are some "hacks" in place to allow you to do everything
except process managment on OS X.

If you use Windows, you're fired.

### Installing Go

http://golang.org/doc/install.html

### Installing junta

    $ git clone git@github.com:bmizerany/junta.git
    $ cd junta
    $ make


You can test it works with:

    $ juntad

Point your browser at http://127.0.0.1:8080

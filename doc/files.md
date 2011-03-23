# Doozer's Files

These follow a simple rule: doozer reserves the right to read and write
paths in `/ctl`, and the details of those paths will be documented;
it will never read or write other paths unless explicitly asked to.

    /ctl/cal   CAL slots
    /ctl/err   mutation errors are written here
    /ctl/link  ephemereal path session links
      (e.g. /ctl/link/foo=abc links /foo to session abc)
    /ctl/node  node metadata
    /ctl/sess  client session files

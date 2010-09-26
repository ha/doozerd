Junta Monitor
=============

First, this is largely modeled on systemd. Go read all their documentation and
you will find few surprises here.

(I'll elide the `/j/<cluster>` path prefix for this document.)

Units are defined in `/mon/unit/<id>` e.g. `/mon/unit/beanstalkd.service`.
Each unit has a name and a type, and its key in the store is the name and the
type separated by a dot. Possible types currently include *service* and
*socket*; in the future we'll add more types, such as *timer*, *path*.

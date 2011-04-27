# Data Model

## Files

Files in Doozer are organized in a tree, much like in [Unix]().  Each directory
contains a list of names that correspond to other directories or files.  Each file
contains a sequence of bytes that can be read or written. The root of the store
is the directory `/`.  The sequence of names leading from the root to any
given file, including the filename, constitutes a path that uniquely identifies
the file.  Paths are notated by joining all the names with `/` between them.

For example, `/foo/bar/baz` refers to the file `baz`, inside the directory
`bar`, inside the directory `foo`, inside the root directory.

### Naming files

Names are UTF-8 character strings that contain only ASCII letters, numbers, `.`,
or `-`.

NOTE:  This may seem ironic but we are keeping the door open to lift some of the
restrictions.

## Read/Write

Users are limited to whole-file read and writes.  Users can only read a whole
file or write over a whole file.

## Revisions

Changes to the store (i.e. creating, updating, or deleting a file) are applied,
one at a time, in sequence.  Every change creates a new version of the store
with one difference from the previous version.  Every version of the store gets
assigned an integer, its `rev`, one greater than the previous rev.  Previous
revisions are kept for reference until [some time later]().

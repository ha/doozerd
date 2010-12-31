# Client Protocol

We use (a generalized version of) the redis protocol format. See
<http://code.google.com/p/redis/wiki/ProtocolSpecification> for more info.

In our version, elements of a multi-bulk response are not just bulk string
responses, they can be any response type, including integers, "+" lines, "-"
errors, and other multi-bulk responses recursively.

Also, both responses and requests use this generalized format.

Every request is formatted in three parts: opid, verb, data. The verb is a
string, the opid is an integer, and the data varies depending on the verb as
given below.

Example Request (pre-encoded - Ruby syntax):

    [123, "SET", ["/foo" "bar" "0"]]

Every response is formatted in three parts: opid, flags, data. The opid is an
integer, the flags is an integer, and the data varies depending on the
associated request as given below.

Example Response (to the above request)

    [123, 2, "63"]

COMMANDS:

    VERB    DATA                 RETURN DATA
    ----    ----                 -----------------------

    # Close the response opid so that it will never be used again.
    CLOSE                        +OK

    # Save a point-in-time snapshot for future reference.
    SNAP                         sid

    # Ensure that snapshot sid is deleted.
    # It is ok to delete a snapshot that does not exist
    # (either because it has been deleted or it never existed).
    DELSNAP sid                  +OK

    # I'll give you 1 guess.
    SET     [path body cas]      cas

    # Set a paths value to the servers current time + interval ns.
    SETT    [path interval cas]  [t, cas]

    # Get a value from snap `sid`.  If `sid` is `0` then value
    # is taken from current live tree.
    GET     [path sid]           body

    DEL     [path cas]           +OK

    # Increment the servers seqn without mutation.
    NOOP    nil                  +OK

    # Walk tree `sid` SAX style.
    WALK    [glob sid]           [path body cas] ...

    # Watch tree.
    WATCH   glob                 [path body cas] ...

## Response Flags:

    Valid = 1
      This response contains valid data and should be delivered to the caller.

      Usually set, but can be unset, for example, in response to WATCH, as a
      side effect of the CLOSE command, to indicate that the watch has been
      closed, in the absence of a valid watch event.

    Done = 2
      This is the last response for the given opid. No more responses will
      be sent. The client is free to release resources associated with this
      opid.

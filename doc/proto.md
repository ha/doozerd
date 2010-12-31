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

    Closed = 1
      The opid in given in the response is closed.

      The trailing arguments are to be ignored. This indicates to clients that
      they should clean-up any listeners for that opid.

    Last = 2
      Same as above except the trailing arguments are valid.

    (Note: instead of Closed and Last, I'd rather have flags Valid and Done,
    where Valid == (not Closed) and Done == (Closed or Last). That way the
    flags are independent and not redundant. -kr)

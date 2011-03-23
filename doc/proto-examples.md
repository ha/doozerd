
## Examples

(In these examples, we'll use an informal notation
similar to JSON to indicate the contents of structures
sent over the wire.)

### Get

Let's say the client wants to retrieve file `/a`. So it
sends the following:

    {
        tag:  0,
        verb: GET,
        path: "/a",
    }

The server replies

    {
        tag:   0,
        flags: 3, // 3 == valid|done
        rev:   5,
        value: "hello",
    }

### Set and Get

Set usually takes much longer than get, so here we'll
see replies come out of order:

    {
        tag:   0,
        verb:  SET,
        path:  "/a",
        rev:   -1,
        value: "goodbye",
    }

    {
        tag:  1,
        verb: GET,
        path: "/a",
    }

The server replies immediately:

    {
        tag:   1,
        flags: 3, // 3 == valid|done
        rev:   5,
        value: "hello",
    }

Some time later, the set operation finishes:

    {
        tag:   0,
        flags: 3, // 3 == valid|done
        rev:   6,
    }

Now, the client can issue the same get request once
more:

    {
        tag:  1,
        verb: GET,
        path: "/a",
    }

This time, the server replies:

    {
        tag:   1,
        flags: 3, // 3 == valid|done
        rev:   6,
        value: "goodbye",
    }

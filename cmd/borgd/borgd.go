package main

import (
	//"borg"
	"flag"
	"log"
	"os"
)

// Flags
var (
	listenAddr *string = flag.String("l", ":8046", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

// Globals
var (
	logger *log.Logger = log.New(os.Stderr, nil, "borgd: ", log.Lok)
)

func main() {
	flag.Parse()

	logger.Logf("attempting to listen on %s\n", *listenAddr)

	if *attachAddr != "" {
		logger.Logf("attempting to attach to %s\n", *attachAddr)
	}

	// Think of borg events like inotify events.  We're interested in changes to
	// them all.  All events are sent to your instance of borg.  The filtering
	// is done once they've arrived.  Let's make keys look like directories to
	// aid this comparison.
	//
	// Example spec:
	//
	//     /<slug_id>/proc/<type>/<upid>/...
	//
	// Example interactive session:
	//
	//     $ borgd -i -a :9999
	//     >> ls /proc/123_a3c_a12b3c45/beanstalkd/12345/
	//     cmd
	//     env
	//     lock
	//     >> cat /proc/123_a3c_a12b3c45/beanstalkd/12345/*
	//     beanstalkd -l 0.0.0.0 -p 4563
	//     PORT=4563
	//     123.4.5.678:9999
	//     >>
	//
	// Example code:
	//
	//     me, err := borg.ListenAndServe(*listenAddr)
	//     if err != nil {
	//         log.Exitf("listen failed: %v", err)
	//     }
	//
	//     // Handle a specific type of key notification.
	//     // The : signals a named variable part.
	//     me.HandleFunc(
	//         "/proc/:slug/beanstalkd/:upid/lock",
	//         func (msg *borg.Message) {
	//             if msg.Value == myId {
	//                 cmd := beanstalkd ....
	//                 ... launch beanstalkd ...
	//                 me.Echo(cmd, "/proc/<slug>/beanstalkd/<upid>/cmd")
	//             }
	//         },
	//     )
}

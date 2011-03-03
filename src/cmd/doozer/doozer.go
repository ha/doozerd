package main

import (
	"doozer"
	"doozer/client"
	"doozer/proto"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

var (
	addr        = flag.String("a", "127.0.0.1:8046", "the address to bind to")
	showHelp    = flag.Bool("h", false, "show help")
	showVersion = flag.Bool("v", false, "print doozer's version string")
)

type cmd struct {
	f interface{}
	a string // args
	d string // short description
}

var (
	self    = os.Args[0]
	cmds    = map[string]cmd{}
	cmdHelp = map[string]string{}
)

const (
	usage1 = `
Each command takes zero or more options and zero or more arguments.
In addition, there are some global options that can be used with any command.
The exit status is 0 on success, or an error code on failure.

Global Options:
`
	usage2 = `
Commands:
`
)


func Usage() {
	fmt.Fprintf(os.Stderr, "Use: %s [options] <command> [options] [args]\n", self)
	fmt.Fprint(os.Stderr, usage1)
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Exit Status (not an exhaustive list):")
	fmt.Fprintf(os.Stderr, "  %3d: %s\n", 0, "success")

	var ns []int
	for n := range proto.Response_Err_name {
		ns = append(ns, int(n))
	}
	sort.SortInts(ns)
	for _, n := range ns {
		name := proto.Response_Err_name[int32(n)]
		switch name {
		// These errors should never be exposed to the user,
		// so don't show them in the usage output.
		case "TAG_IN_USE", "UNKNOWN_VERB", "REDIRECT", "INVALID_SNAP":
			continue
		}
		var s string
		switch name {
		case "NOTDIR":
			s = "not a directory"
		case "ISDIR":
			s = "is a directory"
		default:
			s = strings.Replace(strings.ToLower(name), "_", " ", -1)
		}
		fmt.Fprintf(os.Stderr, "  %3d: %s\n", n, s)
	}

	fmt.Fprint(os.Stderr, usage2)
	var max int
	var names []string
	us := make(map[string]string)
	for k := range cmds {
		u := k + " " + cmds[k].a
		if len(u) > max {
			max = len(u)
		}
		names = append(names, k)
		us[k] = u
	}
	sort.SortStrings(names)
	for _, k := range names {
		fmt.Fprintf(os.Stderr, "  %-*s - %s\n", max, us[k], cmds[k].d)
	}

}


func bail(e os.Error) {
	fmt.Fprintln(os.Stderr, "Error:", e)
	if r, ok := e.(*client.ResponseError); ok {
		os.Exit(int(r.Code))
	}
	os.Exit(127)
}


func mustAtoi64(arg string) int64 {
	cas, err := strconv.Atoi64(arg)
	if err != nil {
		bail(err)
	}
	return cas
}


func main() {
	flag.Usage = Usage
	flag.Parse()

	if *showHelp {
		Usage()
		return
	}

	if *showVersion {
		fmt.Println("doozer", doozer.Version)
		return
	}

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "%s: missing command\n", os.Args[0])
		Usage()
		os.Exit(127)
	}

	cmd := flag.Arg(0)

	c, ok := cmds[cmd]
	if !ok {
		fmt.Fprintln(os.Stderr, "Unknown command:", cmd)
		Usage()
		os.Exit(127)
	}

	os.Args = flag.Args()
	flag.Parse()

	if *showHelp {
		help(cmd)
		return
	}

	args := flag.Args()
	ft := reflect.Typeof(c.f).(*reflect.FuncType)
	if len(args) != ft.NumIn() {
		fmt.Fprintf(os.Stderr, "%s: wrong number of arguments\n", cmd)
		help(cmd)
		os.Exit(127)
	}

	vals := make([]reflect.Value, len(args))
	for i, s := range args {
		vals[i] = reflect.NewValue(s)
	}
	fv := reflect.NewValue(c.f).(*reflect.FuncValue)
	fv.Call(vals)
}

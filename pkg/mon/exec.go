package mon

import (
	"container/vector"
	"fmt"
	"gob"
	"io"
	"os"
	"strconv"
	"syscall"
)

const (
	inputReadFd        = 3
	statusWriteFd      = 4
	passListenFdsStart = 5
)

const cookie = "5561f7ed5c1a1406811f68fa54bdb597" // arbitrary

// Here, on startup, we check if `cookie` has been given to us. If so, we
// behave as a fork/exec helper and prevent `main` from executing.
func init() {
	if len(os.Args) != 1 || os.Args[0] != cookie {
		return
	}

	execInChild()
	panic("unreached")
}

func exitedCleanly(w *os.Waitmsg) bool {
	return w.Exited() && w.ExitStatus() == 0
}

type childError struct {
	os.Error
}

func (e childError) String() string {
	return "child error: " + e.Error.String()
}

// Type exec defines an execution context that can be used for one or more
// commands.
type execCtx struct {
	WorkingDirectory string
	User, Group      string
	Nice             int

	cmd string
}

// ForkExec runs `cmd` in a child process, with all configuration options as
// specified in `ec` and the listen fds `lf` in its initial set of fds.
//
// This is not easy to implement, because Go does not give us a plain fork
// function. Here's the trick: we fork/exec the same program currently running
// in this process, then send configuration parameters to it over a pipe.
//
// After the first exec, the child process will be running this program,
// almost as if we had simply forked, but with a fresh address space. Once the
// child reads the configuration data from its pipe, it has enough information
// to set up the process environment and exec a second time, starting the
// program we really want.
func (ec *execCtx) ForkExec(cmd string, lf []*os.File) (pid int, err os.Error) {
	ec.cmd = cmd
	defer func() {
		ec.cmd = ""
	}()

	var ir, iw, sr, sw *os.File
	sb := make([]byte, 4)

	// These pipe fds are already close-on-exec, which is what we want for iw
	// and sr. It's also okay for ir and sw, because we explicitly supply them
	// to os.ForkExec.
	ir, iw, err = os.Pipe()
	if err != nil {
		return
	}
	defer ir.Close()
	defer iw.Close()
	sr, sw, err = os.Pipe()
	if err != nil {
		return
	}
	defer sr.Close()
	defer sw.Close()

	// Boring, ugly code to set up the list of child fds.
	files := make([]*os.File, passListenFdsStart+len(lf))
	files[0] = os.Stdin
	files[1] = os.Stdout
	files[2] = os.Stderr
	files[inputReadFd] = ir
	files[statusWriteFd] = sw
	copy(files[passListenFdsStart:], lf)

	// Boring, ugly code to set up the child environment vars.
	envv := vector.StringVector(os.Environ())

	// See if we are giving it any listen fds.
	if lf != nil {
		envv.Push(fmt.Sprintf("LISTEN_FDS=%d", len(lf)))
	}

	// Okay, here we go...

	// Fork and exec the same program we're currently running. Give it `cookie`
	// as its only arg, telling it to behave as our helper program and run
	// function `execInChild`.
	pid, err = os.ForkExec("/proc/self/exe", []string{cookie}, envv, "", files)
	if err != nil {
		return 0, err
	}

	// Send it the configuration data.
	en := gob.NewEncoder(iw)
	err = en.Encode(&ec)
	if err != nil {
		goto parenterror
	}

	// Be sure not to deadlock if the child is successful.
	sw.Close()

	// Check if the child had an error.
	var n int
	n, err = sr.Read(sb)
	if err != os.EOF || n != 0 {
		// got a full error code?
		if n == len(sb) {
			// decode it (big-endian)
			errno := 0 | // this 0 is to trick gofmt
				(int32(sb[0]) << 24) |
				(int32(sb[1]) << 16) |
				(int32(sb[2]) << 8) |
				(int32(sb[3]) << 0)
			err = &childError{os.Errno(errno)}
		}
		if err == nil {
			err = os.EPIPE
		}
		goto parenterror
	}

	// Read got EOF, so status pipe closed on exec, so exec succeeded.
	return pid, nil

parenterror:
	// error of some sort after creating the child

	// make sure the child is well and truly dead
	syscall.Kill(pid, syscall.SIGKILL)

	// wait for it to exit, so we don't accumulate zombies
	var wstatus syscall.WaitStatus
	_, e1 := syscall.Wait4(pid, &wstatus, 0, nil)
	for e1 == syscall.EINTR {
		_, e1 = syscall.Wait4(pid, &wstatus, 0, nil)
	}
	return 0, err
}

// Function `execInChild` runs in the child process. It sets up the process
// environment and execs the new program. If there was an error setting up the
// environment, we communicate the error back to the parent on a pipe, then
// exit.
//
// This function never returns.
func execInChild() {
	var ir, sw *os.File

	// See $GOROOT/src/pkg/syscall/exec_unix.go for description of ForkLock.
	// We really don't want this process to fork before we exec, so to be
	// extra-paranoid, we grab the ForkLock and never let go. This should be
	// unnecessary since all init functions run in a single goroutine -- we are
	// guaranteed nothing else is executing in this process.
	syscall.ForkLock.RLock()

	// First, move statusWriteFd and inputReadFd out of the way, so we can
	// shift down all the listen fds.
	swfd, e1 := syscall.Dup(statusWriteFd)
	if e1 != 0 {
		panicChild(os.NewFile(statusWriteFd, "|status"), e1)
	}
	sw = os.NewFile(swfd, "|status")
	irfd, e1 := syscall.Dup(inputReadFd)
	if e1 != 0 {
		panicChild(sw, e1)
	}
	ir = os.NewFile(irfd, "input|")

	// Be sure to set them close-on-exec so the child doesn't inherit them.
	// Also, closing sw on exec is how we notify the parent we were successful.
	syscall.CloseOnExec(swfd)
	syscall.CloseOnExec(irfd)

	// How many (if any) listen fds are we giving to the child?
	nListenFds, _ := strconv.Atoi(os.Getenv("LISTEN_FDS"))

	// Shift down all the listen fds from starting at 5 to start at 3.
	for i := 0; i < nListenFds; i++ {
		// First mark the old one close-on-exec
		syscall.CloseOnExec(i + 5)

		// Now make the new fd (i+3); it is NOT close-on-exec.
		e1 = dup2(i+5, i+3)
		if e1 != 0 {
			panicChild(sw, e1)
		}
	}

	// If we are passing listen fds, also set LISTEN_PID to the pid of the main
	// process (i.e. this one).
	if nListenFds > 0 {
		os.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
	}

	// Now read in the configuration data.
	var ctx execCtx
	gd := gob.NewDecoder(ir)
	err := gd.Decode(&ctx)
	if err != nil {
		panicChild(sw, syscall.EINVAL)
	}

	// Finally, exec the real program. If Exec returns an error, such as
	// file-not-found, we send that back to the parent before exiting.
	envv := os.Environ()
	e1 = syscall.Exec(ctx.cmd, []string{ctx.cmd}, envv)
	panicChild(sw, e1)
}

// Encode the errno (big-endian) and write it to the status pipe.
func panicChild(w io.Writer, errno int) {
	b := []byte{
		byte((int32(errno) >> 24) & 0xff),
		byte((int32(errno) >> 16) & 0xff),
		byte((int32(errno) >> 8) & 0xff),
		byte((int32(errno) >> 0) & 0xff),
	}
	w.Write(b)
	for {
		os.Exit(111)
	}
}

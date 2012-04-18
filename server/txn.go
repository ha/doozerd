package server

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/ha/doozerd/consensus"
	"github.com/ha/doozerd/store"
	"io"
	"log"
	"sort"
	"syscall"
)

type txn struct {
	c    *conn
	req  request
	resp response
}

var ops = map[int32]func(*txn){
	int32(request_DEL):    (*txn).del,
	int32(request_GET):    (*txn).get,
	int32(request_GETDIR): (*txn).getdir,
	int32(request_NOP):    (*txn).nop,
	int32(request_REV):    (*txn).rev,
	int32(request_SET):    (*txn).set,
	int32(request_STAT):   (*txn).stat,
	int32(request_WAIT):   (*txn).wait,
	int32(request_WALK):   (*txn).walk,
	int32(request_ACCESS): (*txn).access,
}

// response flags
const (
	_ = 1 << iota
	_
	set
	del
)

func (t *txn) run() {
	verb := proto.GetInt32((*int32)(t.req.Verb))
	if f, ok := ops[verb]; ok {
		f(t)
	} else {
		t.respondErrCode(response_UNKNOWN_VERB)
	}
}

func (t *txn) get() {
	if !t.c.raccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if t.req.Path == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	go func() {
		g, err := t.getter()
		if err != nil {
			t.respondOsError(err)
			return
		}

		v, rev := g.Get(*t.req.Path)
		if rev == store.Dir {
			t.respondErrCode(response_ISDIR)
			return
		}

		t.resp.Rev = &rev
		if len(v) == 1 { // not missing
			t.resp.Value = []byte(v[0])
		}
		t.respond()
	}()
}

func (t *txn) set() {
	if !t.c.waccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if !t.c.canWrite {
		t.respondErrCode(response_READONLY)
		return
	}

	if t.req.Path == nil || t.req.Rev == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	go func() {
		ev := consensus.Set(t.c.p, *t.req.Path, t.req.Value, *t.req.Rev)
		if ev.Err != nil {
			t.respondOsError(ev.Err)
			return
		}
		t.resp.Rev = &ev.Seqn
		t.respond()
	}()
}

func (t *txn) del() {
	if !t.c.waccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if !t.c.canWrite {
		t.respondErrCode(response_READONLY)
		return
	}

	if t.req.Path == nil || t.req.Rev == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	go func() {
		ev := consensus.Del(t.c.p, *t.req.Path, *t.req.Rev)
		if ev.Err != nil {
			t.respondOsError(ev.Err)
			return
		}
		t.respond()
	}()
}

func (t *txn) nop() {
	if !t.c.waccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if !t.c.canWrite {
		t.respondErrCode(response_READONLY)
		return
	}

	go func() {
		t.c.p.Propose([]byte(store.Nop))
		t.respond()
	}()
}

func (t *txn) rev() {
	rev := <-t.c.st.Seqns
	t.resp.Rev = &rev
	t.respond()
}

func (t *txn) stat() {
	if !t.c.raccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	go func() {
		g, err := t.getter()
		if err != nil {
			t.respondOsError(err)
			return
		}

		len, rev := g.Stat(proto.GetString(t.req.Path))
		t.resp.Len = &len
		t.resp.Rev = &rev
		t.respond()
	}()
}

func (t *txn) getdir() {
	if !t.c.raccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if t.req.Path == nil || t.req.Offset == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	go func() {
		g, err := t.getter()
		if err != nil {
			t.respondOsError(err)
			return
		}

		ents, rev := g.Get(*t.req.Path)
		if rev == store.Missing {
			t.respondErrCode(response_NOENT)
			return
		}
		if rev != store.Dir {
			t.respondErrCode(response_NOTDIR)
			return
		}

		sort.Strings(ents)
		offset := int(*t.req.Offset)
		if offset < 0 || offset >= len(ents) {
			t.respondErrCode(response_RANGE)
			return
		}

		t.resp.Path = &ents[offset]
		t.respond()
	}()
}

func (t *txn) wait() {
	if !t.c.raccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if t.req.Path == nil || t.req.Rev == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	glob, err := store.CompileGlob(*t.req.Path)
	if err != nil {
		t.respondOsError(err)
		return
	}

	ch, err := t.c.st.Wait(glob, *t.req.Rev)
	if err != nil {
		t.respondOsError(err)
		return
	}

	go func() {
		ev := <-ch
		t.resp.Path = &ev.Path
		t.resp.Value = []byte(ev.Body)
		t.resp.Rev = &ev.Seqn
		switch {
		case ev.IsSet():
			t.resp.Flags = proto.Int32(set)
		case ev.IsDel():
			t.resp.Flags = proto.Int32(del)
		default:
			t.resp.Flags = proto.Int32(0)
		}
		t.respond()
	}()
}

func (t *txn) walk() {
	if !t.c.raccess {
		t.respondOsError(syscall.EACCES)
		return
	}

	if t.req.Path == nil || t.req.Offset == nil {
		t.respondErrCode(response_MISSING_ARG)
		return
	}

	glob, err := store.CompileGlob(*t.req.Path)
	if err != nil {
		t.respondOsError(err)
		return
	}

	offset := *t.req.Offset
	if offset < 0 {
		t.respondErrCode(response_RANGE)
		return
	}

	go func() {
		g, err := t.getter()
		if err != nil {
			t.respondOsError(err)
			return
		}

		f := func(path, body string, rev int64) (stop bool) {
			if offset == 0 {
				t.resp.Path = &path
				t.resp.Value = []byte(body)
				t.resp.Rev = &rev
				t.resp.Flags = proto.Int32(set)
				t.respond()
				return true
			}
			offset--
			return false
		}
		if !store.Walk(g, glob, f) {
			t.respondErrCode(response_RANGE)
		}
	}()
}

func (t *txn) access() {
	if t.c.grant(string(t.req.Value)) {
		t.respond()
	} else {
		t.respondOsError(syscall.EACCES)
	}
}

func (t *txn) respondOsError(err error) {
	switch err {
	case store.ErrBadPath:
		t.respondErrCode(response_BAD_PATH)
	case store.ErrRevMismatch:
		t.respondErrCode(response_REV_MISMATCH)
	case store.ErrTooLate:
		t.respondErrCode(response_TOO_LATE)
	case syscall.EISDIR:
		t.respondErrCode(response_ISDIR)
	case syscall.ENOTDIR:
		t.respondErrCode(response_NOTDIR)
	default:
		t.resp.ErrDetail = proto.String(err.Error())
		t.respondErrCode(response_OTHER)
	}
}

func (t *txn) respondErrCode(e response_Err) {
	t.resp.ErrCode = &e
	t.respond()
}

func (t *txn) respond() {
	t.resp.Tag = t.req.Tag
	err := t.c.write(&t.resp)
	if err != nil && err != io.EOF {
		log.Println(err)
	}
}

func (t *txn) getter() (store.Getter, error) {
	if t.req.Rev == nil {
		_, g := t.c.st.Snap()
		return g, nil
	}

	ch, err := t.c.st.Wait(store.Any, *t.req.Rev)
	if err != nil {
		return nil, err
	}
	return <-ch, nil
}

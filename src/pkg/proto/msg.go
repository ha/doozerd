package proto

// GET

type ReqGet struct {
	Path   string
	SnapId int
}

type ResGet struct {
	V   []string
	Cas string
}

// SET

type ReqSet struct {
	Path, Body, Cas string
}

// SETT

type ReqSett struct {
	Path     string
	Interval int64
	Cas      string
}

type ResSett struct {
	Exp int64
	Cas string
}

// DEL

type ReqDel struct {
	Path, Cas string
}

// JOIN
// e.g. join 4eec5bfb.38c24ce9 1.2.3.4:999

type ReqJoin struct {
	Who, Addr string
}

type ResJoin struct {
	Seqn     uint64
	Snapshot string
}

// CHECKIN

type ReqCheckin struct {
	Sid, Cas string
}

type ResCheckin struct {
	Exp int64
	Cas string
}

// WATCH

type ResWatch struct {
	Path, Body, Cas string
}

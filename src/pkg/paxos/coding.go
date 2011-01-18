package paxos

import (
	pb "goprotobuf.googlecode.com/hg/proto"
)

type WriterTo interface {
	WriteTo(data []byte, addr string)
}


type Encoder struct {
	WriterTo
}


func (e Encoder) PutTo(m *M, addr string) {
	m.WireFrom = nil

	buf, err := pb.Marshal(m)
	if err != nil {
		logger.Println(err)
		return
	}

	e.WriteTo(buf, addr)
}


type PutterFrom interface {
	PutFrom(addr string, m *M)
}


type Decoder struct {
	PutterFrom
}


func (d Decoder) WriteFrom(addr string, data []byte) {
	var m M
	err := pb.Unmarshal(data, &m)
	if err != nil {
		logger.Println(err)
		return
	}

	d.PutFrom(addr, &m)
}

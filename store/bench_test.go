package store

import (
	"testing"
)


func BenchmarkHist10(b *testing.B) {
	benchmarkNHist(10, b)
}


func BenchmarkHist1e3(b *testing.B) {
	benchmarkNHist(1e3, b)
}


func BenchmarkHist1e4(b *testing.B) {
	benchmarkNHist(1e4, b)
}


func BenchmarkHist1e5(b *testing.B) {
	benchmarkNHist(1e5, b)
}


func BenchmarkHist1e6(b *testing.B) {
	benchmarkNHist(1e6, b)
}


func benchmarkNHist(n int, b *testing.B) {
	b.StopTimer()
	st := New("")
	mut := [...]string{
		MustEncodeSet("/test/path/one/foo", "12345", Clobber),
		MustEncodeSet("/test/path/two/foo", "23456", Clobber),
		MustEncodeSet("/test/path/three/foo", "34567", Clobber),
	}
	for i := 0; i < n; i++ {
		st.Ops <- Op{int64(i), mut[i%len(mut)], false}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		st.Ops <- Op{int64(n + i), mut[i%len(mut)], false}
		st.Clean(int64(i))
	}
	b.StopTimer()
	close(st.Ops)
}

package proto

import (
	"reflect"
	"testing"
)

type fitTest struct {
	val, slot, exp interface{}
}

type T struct {
	I int
	S string
	B []byte
}

type U struct {
	X T
	Y *T
}

type V struct {
	X **T
}

// These are here because &&T{} is invalid Go.
var (
	hi   = []byte{'h', 'i'}
	phi  = &hi
	shi  = string(hi)
	pshi = &shi
	pt   = &T{1, string(hi), hi}
	u1   = uint64(1)
	pu1  = &u1
)

var fitTests = []fitTest{
	{int64(1), new(int), int(1)},
	{int64(1), new(int8), int8(1)},
	{int64(1), new(int16), int16(1)},
	{int64(1), new(int32), int32(1)},
	{int64(1), new(int64), int64(1)},
	{int64(1), new(uint), uint(1)},
	{int64(1), new(uint8), uint8(1)}, // aka byte
	{int64(1), new(uint16), uint16(1)},
	{int64(1), new(uint32), uint32(1)},
	{int64(1), new(uint64), uint64(1)},
	{uint64(1), new(int), int(1)},
	{uint64(1), new(int8), int8(1)},
	{uint64(1), new(int16), int16(1)},
	{uint64(1), new(int32), int32(1)},
	{uint64(1), new(int64), int64(1)},
	{uint64(1), new(uint), uint(1)},
	{uint64(1), new(uint8), uint8(1)}, // aka byte
	{uint64(1), new(uint16), uint16(1)},
	{uint64(1), new(uint32), uint32(1)},
	{uint64(1), new(uint64), uint64(1)},
	{hi, new(string), "hi"},
	{nil, new(string), ""},
	{hi, new([]byte), hi},
	{hi, new(*[]byte), &hi},
	{hi, new(**[]byte), &phi},

	{[]interface{}{hi, hi}, new(interface{}), []interface{}{hi, hi}},
	{ResponseError("hi"), new(interface{}), ResponseError("hi")},
	{Redirect("hi"), new(interface{}), Redirect("hi")},

	{nil, &T{I: 1}, T{}},
	{[]interface{}{int64(1), hi, hi}, &T{}, T{1, string(hi), hi}},
	{[]interface{}{int64(1), hi, hi}, new(T), T{1, string(hi), hi}},
	{[]interface{}{int64(1), hi, hi}, new(*T), &T{1, string(hi), hi}},

	{[]interface{}{[]interface{}{int64(1), hi, hi},
		[]interface{}{int64(1), hi, hi}},
		&U{},
		U{T{1, string(hi), hi}, &T{1, string(hi), hi}}},

	{[]interface{}{nil, nil}, &U{T{}, &T{}}, U{T{}, nil}},

	{[]interface{}{[]interface{}{int64(1), hi, hi}}, &V{}, V{&pt}},

	{uint64(1), new(*uint64), &u1},
	{uint64(1), new(**uint64), &pu1},
	{[]interface{}{hi, hi}, new([]string), []string{"hi", "hi"}},
	{[]interface{}{hi, hi}, new([]*string), []*string{pshi, pshi}},
	{[]interface{}{hi, hi}, new([]**string), []**string{&pshi, &pshi}},
	{[]interface{}{1, 1}, new([2]int), [2]int{1, 1}},
}

var fitErrors = []fitTest{
	{int64(5e9), new(int32), nil},                 // out of range
	{uint64(5e9), new(uint32), nil},               // out of range
	{int64(5e9), new(uint32), nil},                // out of range
	{uint64(5e9), new(int32), nil},                // out of range
	{int64(-5), new(uint64), nil},                 // out of range
	{uint64(0xfffffffffffffffb), new(int64), nil}, // out of range

	{[]interface{}{hi, hi}, new(interface {
		a()
	}),
		nil},

	{[]interface{}{int64(1), hi, hi}, *new(*T), nil},

	{[]interface{}{1, 1}, new([1]int), nil},
	{[]interface{}{1, 1}, new([3]int), nil},
}

func TestFitNil(t *testing.T) {
	err := Fit(nil, nil)
	if err != nil {
		t.Error("unexpected error:", err)
	}
}

func TestFitVal(t *testing.T) {
	for i, f := range fitTests {
		i++

		v := reflect.NewValue(f.slot)
		rt := v.Type().(*reflect.PtrType).Elem()
		if !assignable(rt, reflect.Typeof(f.exp)) {
			t.Errorf("#%d oops, test bug, type %T cannot -> type %v", i, f.exp, rt)
			continue
		}

		// First test fitting to a concrete type.
		err := Fit(f.val, f.slot)
		if err != nil {
			t.Errorf("#%d unexpected err: %v", i, err)
			continue
		}

		got := reflect.Indirect(v).Interface()
		if !reflect.DeepEqual(f.exp, got) {
			t.Errorf("#%d from %#v", i, f.val)
			t.Errorf("expected %T, %#v", f.exp, f.exp)
			t.Errorf("     got %T, %#v", got, got)
			continue
		}

		// Also test fitting to the empty interface.
		got = interface{}(nil)
		err = Fit(f.val, &got)
		if err != nil {
			t.Errorf("#%d unexpected err: %v", i, err)
			continue
		}

		exp := f.val
		if !reflect.DeepEqual(exp, got) {
			t.Errorf("#%d from %#v", i, f.val)
			t.Errorf("expected %T, %#v (via interface{})", exp, exp)
			t.Errorf("     got %T, %#v", got, got)
		}
	}
}

func TestFitNewPtr(t *testing.T) {
	var p interface{} = new(*T)

	x := []interface{}{1, hi, hi}
	y := []interface{}{2, hi, hi}

	err := Fit(x, p)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}

	r := **p.(**T)

	err = Fit(y, p)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}

	s := **p.(**T)

	if reflect.DeepEqual(r, s) {
		t.Errorf("r and s are the same: %v", r)
	}
}

func TestFitErr(t *testing.T) {
	for i, f := range fitErrors {
		i++

		err := Fit(f.val, f.slot)
		if (f.exp != nil && err != f.exp) || err == nil {
			r := reflect.Indirect(reflect.NewValue(f.slot)).Type()
			t.Errorf("#%d wanted err from %v(%#v), but got none", i, r, f.val)
			if f.exp != nil {
				t.Errorf("   looking for %T %v", f.exp, f.exp)
			}
			continue
		}
	}
}

// See http://golang.org/doc/go_spec.html#Assignability
// This implementation isn't correct, but it covers
// enough cases for us.
func assignable(d, s reflect.Type) bool {
	return d == s || d.(*reflect.InterfaceType).NumMethod() == 0
}

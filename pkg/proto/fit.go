package proto

import (
	"fmt"
	"math"
	"os"
	"reflect"
)

type FitError struct {
	Desc  string
	Val   interface{}
	Label string
	Type  reflect.Type
}

func (e *FitError) String() string {
	return fmt.Sprintf("%#v does not fit %s %v: %s", e.Val, e.Label, e.Type, e.Desc)
}

// Slot must be a pointer.
func Fit(data, slot interface{}) os.Error {
	v := reflect.NewValue(slot)
	pv, ok := v.(*reflect.PtrValue)
	if !ok {
		return &FitError{"not a pointer", data, "slot", v.Type()}
	}

	return fitValue(data, pv.Elem())
}

func fitValue(x interface{}, v reflect.Value) os.Error {
	pv, ok := v.(*reflect.PtrValue)
	for ok {
		if x == nil {
			pv.PointTo(nil)
			return nil
		}
		pv.PointTo(reflect.MakeZero(pv.Type().(*reflect.PtrType).Elem()))
		v = pv.Elem()
		pv, ok = v.(*reflect.PtrValue)
	}

	// Same type? No conversion is necessary.
	xv := reflect.NewValue(x)
	if xv != nil && v != nil && xv.Type() == v.Type() {
		v.SetValue(xv)
		return nil
	}

	// Going to the empty interface? No conversion is necessary.
	iv, ok := v.(*reflect.InterfaceValue)
	if ok && iv.Type().(*reflect.InterfaceType).NumMethod() == 0 {
		iv.Set(reflect.NewValue(x))
		return nil
	}

	switch t := x.(type) {
	case int64:
		iv, ok := v.(*reflect.IntValue)
		if ok {
			if iv.Overflow(t) {
				return os.ERANGE
			}
			iv.Set(t)
			break
		}
		uv, ok := v.(*reflect.UintValue)
		if ok {
			if t < 0 || uv.Overflow(uint64(t)) {
				return os.ERANGE
			}
			uv.Set(uint64(t))
			break
		}
		return &FitError{"cannot hold an integer", t, "slot", v.Type()}
	case uint64:
		uv, ok := v.(*reflect.UintValue)
		if ok {
			if uv.Overflow(t) {
				return os.ERANGE
			}
			uv.Set(t)
			break
		}
		iv, ok := v.(*reflect.IntValue)
		if ok {
			if t > math.MaxInt64 || iv.Overflow(int64(t)) {
				return os.ERANGE
			}
			iv.Set(int64(t))
			break
		}
		return &FitError{"cannot hold an integer", t, "slot", v.Type()}
	case []byte:
		return fitBytes(t, v)
	case ResponseError:
		return t
	case nil:
		v.SetValue(reflect.MakeZero(v.Type()))
	case []interface{}:
		return fitSeq(t, v)
	default:
		return &FitError{"unknown source type", x, "slot", v.Type()}
	}

	return nil
}

// For conversion only. If the types match exactly, fitValue handles fitting.
func fitBytes(b []byte, v reflect.Value) os.Error {
	switch t := v.(type) {
	case *reflect.StringValue:
		t.Set(string(b))
	default:
		return &FitError{"cannot hold a string", b, "slot", v.Type()}
	}

	return nil
}

// For conversion only. If the types match exactly, fitValue handles fitting.
func fitSeq(s []interface{}, v reflect.Value) os.Error {
	switch t := v.(type) {
	case *reflect.StructValue:
		if len(s) != t.NumField() {
			return &FitError{"arity mismatch", s, "struct" , v.Type()}
		}
		for i := range s {
			f := t.Type().(*reflect.StructType).Field(i)
			if f.PkgPath != "" {
				return &FitError{"private field", s[i], f.Name, f.Type}
			}
			err := fitValue(s[i], t.Field(i))
			if err != nil {
				return err
			}
		}
	case *reflect.ArrayValue:
		if len(s) != t.Len() {
			return &FitError{"arity mismatch", s, "array" , v.Type()}
		}
		for i := range s {
			err := fitValue(s[i], t.Elem(i))
			if err != nil {
				return err
			}
		}
	case *reflect.SliceValue:
		t.Set(reflect.MakeSlice(t.Type().(*reflect.SliceType), len(s), len(s)))
		for i := range s {
			err := fitValue(s[i], t.Elem(i))
			if err != nil {
				return err
			}
		}
	default:
		if v == nil {
			return &FitError{"cannot hold a sequence", s, "slot", nil}
		}
		return &FitError{"cannot hold a sequence", s, "slot", v.Type()}
	}

	return nil
}

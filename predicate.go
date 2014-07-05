// predicate defines logical predicates used in relation's restrict

package rel

import (
	"fmt"
	"reflect"
	"strings"
)

// Predicate is the type of func that takes a tuple and returns bool and is
// used for restrict.  It should always be a func with input of a subdomain
// of the relation, with one bool output.
type Predicate interface {
	// EvalFunc returns a function which can evalutes a predicate on an input
	// tuple
	EvalFunc(e reflect.Type) func(t interface{}) bool

	// Domain is the type of input that is required to evalute the predicate
	// this might have to be a recursive type instead of reflect.Type?
	Domain() []Attribute

	// And two predicates
	And(p2 Predicate) AndPred

	// Or two predicates
	Or(p2 Predicate) OrPred

	// Xor two predicates
	Xor(p2 Predicate) XorPred

	String() string
}

// unionAttributes produces a union of two sets of attributes, without dups
// assuming that the input attributes are already unique. This returns a copy
// and does not modify the inputs.
func unionAttributes(att1 []Attribute, att2 []Attribute) []Attribute {
	// For small sets of attributes (which should be typical!) this should be
	// faster than a map.
	att := make([]Attribute, len(att1))
	copy(att, att1)
Found:
	for _, v2 := range att2 {
		for _, v1 := range att1 {
			if v1 == v2 {
				continue Found
			}
		}
		att = append(att, v2)
	}
	return att
}

// Not predicate
func Not(p Predicate) NotPred {
	// Prefix not is a lot more comprehensible than postfix!  To that end, it
	// is not a part of the interface because that would require postfix.
	return NotPred{p}
}

// NotPred represents a logical not of a predicate
type NotPred struct {
	P Predicate
}

// String representation of Not
func (p1 NotPred) String() string {
	return fmt.Sprintf("!(%v)", p1.P)
}

// Domain is the type of input that is required to evalute the predicate
func (p1 NotPred) Domain() []Attribute {
	return p1.P.Domain()
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 NotPred) EvalFunc(e reflect.Type) func(t interface{}) bool {
	f := p1.P.EvalFunc(e)
	return func(t interface{}) bool { return !f(t) }
}

// And predicate
func (p1 NotPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 NotPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 NotPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// AndPred represents a logical and predicate
type AndPred struct {
	P1 Predicate
	P2 Predicate
}

// String representation of And
func (p1 AndPred) String() string {
	return fmt.Sprintf("(%v) && (%v)", p1.P1, p1.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p1 AndPred) Domain() []Attribute {
	return unionAttributes(p1.P1.Domain(), p1.P2.Domain())
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 AndPred) EvalFunc(e reflect.Type) func(t interface{}) bool {
	f1 := p1.P1.EvalFunc(e)
	f2 := p1.P2.EvalFunc(e)
	return func(t interface{}) bool { return f1(t) && f2(t) }
}

// And predicate
func (p1 AndPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 AndPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 AndPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// OrPred represents a logical or predicate
type OrPred struct {
	P1 Predicate
	P2 Predicate
}

// String representation of Or
func (p1 OrPred) String() string {
	return fmt.Sprintf("(%v) || (%v)", p1.P1, p1.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p1 OrPred) Domain() []Attribute {
	return unionAttributes(p1.P1.Domain(), p1.P2.Domain())
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 OrPred) EvalFunc(e reflect.Type) func(t interface{}) bool {

	f1 := p1.P1.EvalFunc(e)
	f2 := p1.P2.EvalFunc(e)
	return func(t interface{}) bool { return f1(t) || f2(t) }
}

// And predicate
func (p1 OrPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 OrPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 OrPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// XorPred represents a logical xor predicate
type XorPred struct {
	P1 Predicate
	P2 Predicate
}

// String representation of Xor
func (p1 XorPred) String() string {
	return fmt.Sprintf("(%v) != (%v)", p1.P1, p1.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p1 XorPred) Domain() []Attribute {
	return unionAttributes(p1.P1.Domain(), p1.P2.Domain())
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 XorPred) EvalFunc(e reflect.Type) func(t interface{}) bool {
	f1 := p1.P1.EvalFunc(e)
	f2 := p1.P2.EvalFunc(e)
	return func(t interface{}) bool {
		return f1(t) != f2(t)
	}
}

// And predicate
func (p1 XorPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 XorPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 XorPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// AdHoc is a Predicate that can implement any function on a tuple.
// The rewrite engine will be able to infer which attributes it requires to be
// evaluated, but nothing beyond that, which will prevent it from being moved
// into source queries in e.g. sql.  For those kind of predicates, non AdHoc
// predicates will be required.
// I expect that this will typically be constructed with anonymous functions.
type AdHoc struct {
	// f is the function which takes a tuple and returns a boolean indicating
	// that the tuple passes the predicate
	F interface{}
}

// String representation of AdHoc
func (p1 AdHoc) String() string {
	dom := p1.Domain()
	s := make([]string, len(dom))
	for i, v := range dom {
		s[i] = string(v)
	}
	return fmt.Sprintf("func({%s})", strings.Join(s, ", "))
	// Note we could use
	// f := runtime.FuncForPC(reflect.ValueOf(p.f).Pointer()).Name()
	// for named functions, but the name would require more manipulation to get
	// to a useful brevity, and also would require some work to distinguish
	// between named and anonymous functions.

}

// Domain is the type of input that is required to evalute the predicate
func (p1 AdHoc) Domain() []Attribute {
	return FieldNames(reflect.TypeOf(p1.F).In(0))
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 AdHoc) EvalFunc(e1 reflect.Type) func(t interface{}) bool {

	e2 := reflect.TypeOf(p1.F).In(0)

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := FieldMap(e1, e2)
	pf := reflect.ValueOf(p1.F)

	return func(tup1 interface{}) bool {
		tup2 := reflect.Indirect(reflect.New(e2))
		rtup1 := reflect.ValueOf(tup1)
		for _, fm := range fMap {
			tupf2 := tup2.Field(fm.J)
			tupf2.Set(rtup1.Field(fm.I))
		}

		parm := make([]reflect.Value, 1)
		parm[0] = tup2
		b := pf.Call(parm)
		return b[0].Interface().(bool)
	}
}

// And predicate
func (p1 AdHoc) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 AdHoc) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 AdHoc) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// Normal go style does not include abbreviations or all caps.  However, in
// this case I believe the shortness of the function name is paramount.  I've
// chosen the MIPS assembly condition names as a guide for the names of the
// comparisons.  I would have used x86 but it has a lot of single character
// names and that would be a bit too short for me.  Look at
// http://logos.cs.uic.edu/366/notes/mips%20quick%20tutorial.htm for a
// reference.
//
// The v param is an interface because it might be a literal, or another
// attribute.
// Note that you can have a runtime error if the predicate's literal has the
// wrong type, which is particularly important with ints and floats.

// EQPred is a representation of equal to (==)
type EQPred struct {
	att []Attribute
	lit interface{}
}

// String representation of EQ
func (p1 EQPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v == %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v == %v", p1.att[0], p1.lit)
}

// EQ Equal to (==)
func (att1 Attribute) EQ(v interface{}) EQPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return EQPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return EQPred{att, v}
}

// Domain is the type of input that is required to evalute the predicate
func (p1 EQPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 EQPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {
	// The only method defined on all interfaces is equal & not equal.
	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() == rtup1.FieldByName(att2).Interface()
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		return rtup1.FieldByName(att1).Interface() == p1.lit
	}
}

// And predicate
func (p1 EQPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 EQPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 EQPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// LTPred is a representation of less than (<)
type LTPred struct {
	att []Attribute
	lit interface{}
}

// String representation of LT
func (p1 LTPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v < %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v < %v", p1.att[0], p1.lit)
}

// LT Less than (<)
func (att1 Attribute) LT(v interface{}) LTPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return LTPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return LTPred{att, v}
}

// Domain is the type of input that is required to evalute the predicate
func (p1 LTPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 LTPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			f2 := rtup1.FieldByName(att2).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) < f2.(int)
			case string:
				return f1.(string) < f2.(string)
			case uint8:
				return f1.(uint8) < f2.(uint8)
			case uint16:
				return f1.(uint16) < f2.(uint16)
			case uint32:
				return f1.(uint32) < f2.(uint32)
			case uint64:
				return f1.(uint64) < f2.(uint64)
			case int8:
				return f1.(int8) < f2.(int8)
			case int16:
				return f1.(int16) < f2.(int16)
			case int32:
				return f1.(int32) < f2.(int32)
			case int64:
				return f1.(int64) < f2.(int64)
			case float32:
				return f1.(float32) < f2.(float32)
			case float64:
				return f1.(float64) < f2.(float64)
			}
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		f1 := rtup1.FieldByName(att1).Interface()
		switch f1.(type) {
		default:
			// I am _REALLY_ unsure this is the desired behavior
			return false
		case int:
			return f1.(int) < p1.lit.(int)
		case string:
			return f1.(string) < p1.lit.(string)
		case uint8:
			return f1.(uint8) < p1.lit.(uint8)
		case uint16:
			return f1.(uint16) < p1.lit.(uint16)
		case uint32:
			return f1.(uint32) < p1.lit.(uint32)
		case uint64:
			return f1.(uint64) < p1.lit.(uint64)
		case int8:
			return f1.(int8) < p1.lit.(int8)
		case int16:
			return f1.(int16) < p1.lit.(int16)
		case int32:
			return f1.(int32) < p1.lit.(int32)
		case int64:
			return f1.(int64) < p1.lit.(int64)
		case float32:
			return f1.(float32) < p1.lit.(float32)
		case float64:
			return f1.(float64) < p1.lit.(float64)
		}
	}
}

// And predicate
func (p1 LTPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 LTPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 LTPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// LEPred is a representation of less than or equal to (<=)
type LEPred struct {
	att []Attribute
	lit interface{}
}

// LE Less than or equal to (<=)
func (att1 Attribute) LE(v interface{}) LEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return LEPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return LEPred{att, v}
}

// String representation of LE
func (p1 LEPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v <= %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v <= %v", p1.att[0], p1.lit)
}

// Domain is the type of input that is required to evalute the predicate
func (p1 LEPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 LEPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			f2 := rtup1.FieldByName(att2).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) <= f2.(int)
			case string:
				return f1.(string) <= f2.(string)
			case uint8:
				return f1.(uint8) <= f2.(uint8)
			case uint16:
				return f1.(uint16) <= f2.(uint16)
			case uint32:
				return f1.(uint32) <= f2.(uint32)
			case uint64:
				return f1.(uint64) <= f2.(uint64)
			case int8:
				return f1.(int8) <= f2.(int8)
			case int16:
				return f1.(int16) <= f2.(int16)
			case int32:
				return f1.(int32) <= f2.(int32)
			case int64:
				return f1.(int64) <= f2.(int64)
			case float32:
				return f1.(float32) <= f2.(float32)
			case float64:
				return f1.(float64) <= f2.(float64)
			}
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		f1 := rtup1.FieldByName(att1).Interface()
		switch f1.(type) {
		default:
			// I am _REALLY_ unsure this is the desired behavior
			return false
		case int:
			return f1.(int) <= p1.lit.(int)
		case string:
			return f1.(string) <= p1.lit.(string)
		case uint8:
			return f1.(uint8) <= p1.lit.(uint8)
		case uint16:
			return f1.(uint16) <= p1.lit.(uint16)
		case uint32:
			return f1.(uint32) <= p1.lit.(uint32)
		case uint64:
			return f1.(uint64) <= p1.lit.(uint64)
		case int8:
			return f1.(int8) <= p1.lit.(int8)
		case int16:
			return f1.(int16) <= p1.lit.(int16)
		case int32:
			return f1.(int32) <= p1.lit.(int32)
		case int64:
			return f1.(int64) <= p1.lit.(int64)
		case float32:
			return f1.(float32) <= p1.lit.(float32)
		case float64:
			return f1.(float64) <= p1.lit.(float64)
		}
	}
}

// And predicate
func (p1 LEPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 LEPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 LEPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// GTPred is a representation of greater than (>)
type GTPred struct {
	att []Attribute
	lit interface{}
}

// String representation of GT
func (p1 GTPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v > %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v > %v", p1.att[0], p1.lit)
}

// GT Greater than (>)
func (att1 Attribute) GT(v interface{}) GTPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return GTPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return GTPred{att, v}
}

// Domain is the type of input that is required to evalute the predicate
func (p1 GTPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 GTPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			f2 := rtup1.FieldByName(att2).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) > f2.(int)
			case string:
				return f1.(string) > f2.(string)
			case uint8:
				return f1.(uint8) > f2.(uint8)
			case uint16:
				return f1.(uint16) > f2.(uint16)
			case uint32:
				return f1.(uint32) > f2.(uint32)
			case uint64:
				return f1.(uint64) > f2.(uint64)
			case int8:
				return f1.(int8) > f2.(int8)
			case int16:
				return f1.(int16) > f2.(int16)
			case int32:
				return f1.(int32) > f2.(int32)
			case int64:
				return f1.(int64) > f2.(int64)
			case float32:
				return f1.(float32) > f2.(float32)
			case float64:
				return f1.(float64) > f2.(float64)
			}
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		f1 := rtup1.FieldByName(att1).Interface()
		switch f1.(type) {
		default:
			// I am _REALLY_ unsure this is the desired behavior
			return false
		case int:
			return f1.(int) > p1.lit.(int)
		case string:
			return f1.(string) > p1.lit.(string)
		case uint8:
			return f1.(uint8) > p1.lit.(uint8)
		case uint16:
			return f1.(uint16) > p1.lit.(uint16)
		case uint32:
			return f1.(uint32) > p1.lit.(uint32)
		case uint64:
			return f1.(uint64) > p1.lit.(uint64)
		case int8:
			return f1.(int8) > p1.lit.(int8)
		case int16:
			return f1.(int16) > p1.lit.(int16)
		case int32:
			return f1.(int32) > p1.lit.(int32)
		case int64:
			return f1.(int64) > p1.lit.(int64)
		case float32:
			return f1.(float32) > p1.lit.(float32)
		case float64:
			return f1.(float64) > p1.lit.(float64)
		}
	}
}

// And predicate
func (p1 GTPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 GTPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 GTPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// GEPred is a representation of greater than or equal to (>=)
type GEPred struct {
	att []Attribute
	lit interface{}
}

// String representation of GE (>=)
func (p1 GEPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v >= %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v >= %v", p1.att[0], p1.lit)
}

// GE Greater than or equal to (>=)
func (att1 Attribute) GE(v interface{}) GEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return GEPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return GEPred{att, v}
}

// Domain is the type of input that is required to evalute the predicate
func (p1 GEPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 GEPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			f2 := rtup1.FieldByName(att2).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) >= f2.(int)
			case string:
				return f1.(string) >= f2.(string)
			case uint8:
				return f1.(uint8) >= f2.(uint8)
			case uint16:
				return f1.(uint16) >= f2.(uint16)
			case uint32:
				return f1.(uint32) >= f2.(uint32)
			case uint64:
				return f1.(uint64) >= f2.(uint64)
			case int8:
				return f1.(int8) >= f2.(int8)
			case int16:
				return f1.(int16) >= f2.(int16)
			case int32:
				return f1.(int32) >= f2.(int32)
			case int64:
				return f1.(int64) >= f2.(int64)
			case float32:
				return f1.(float32) >= f2.(float32)
			case float64:
				return f1.(float64) >= f2.(float64)
			}
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		f1 := rtup1.FieldByName(att1).Interface()
		switch f1.(type) {
		default:
			// I am _REALLY_ unsure this is the desired behavior
			return false
		case int:
			return f1.(int) >= p1.lit.(int)
		case string:
			return f1.(string) >= p1.lit.(string)
		case uint8:
			return f1.(uint8) >= p1.lit.(uint8)
		case uint16:
			return f1.(uint16) >= p1.lit.(uint16)
		case uint32:
			return f1.(uint32) >= p1.lit.(uint32)
		case uint64:
			return f1.(uint64) >= p1.lit.(uint64)
		case int8:
			return f1.(int8) >= p1.lit.(int8)
		case int16:
			return f1.(int16) >= p1.lit.(int16)
		case int32:
			return f1.(int32) >= p1.lit.(int32)
		case int64:
			return f1.(int64) >= p1.lit.(int64)
		case float32:
			return f1.(float32) >= p1.lit.(float32)
		case float64:
			return f1.(float64) >= p1.lit.(float64)
		}
	}
}

// And predicate
func (p1 GEPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 GEPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 GEPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

// NEPred represents a not equal to (!=) operation
type NEPred struct {
	att []Attribute
	lit interface{}
}

// String representation of NEPred (!-)
func (p1 NEPred) String() string {
	if len(p1.att) == 2 {
		return fmt.Sprintf("%v != %v", p1.att[0], p1.att[1])
	}
	return fmt.Sprintf("%v != %v", p1.att[0], p1.lit)
}

// NE Not equal to (!=)
func (att1 Attribute) NE(v interface{}) NEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return NEPred{att, reflect.Value{}}
	}
	// v is a literal, we'll need runtime reflection
	att := make([]Attribute, 1)
	att[0] = att1
	return NEPred{att, v}
}

// Domain is the type of input that is required to evalute the predicate
func (p1 NEPred) Domain() []Attribute {
	return p1.att
}

// EvalFunc returns a function which evalutes a predicate on an input tuple
func (p1 NEPred) EvalFunc(e1 reflect.Type) func(t interface{}) bool {

	// The only method defined on all interfaces is equal & not equal.

	if len(p1.att) == 2 {
		att1 := string(p1.att[0])
		att2 := string(p1.att[1])
		return func(tup1 interface{}) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() != rtup1.FieldByName(att2).Interface()
		}
	}
	// the second element is a literal
	att1 := string(p1.att[0])
	return func(tup1 interface{}) bool {
		rtup1 := reflect.ValueOf(tup1)
		return rtup1.FieldByName(att1).Interface() != p1.lit
	}
}

// And predicate
func (p1 NEPred) And(p2 Predicate) AndPred {
	return AndPred{p1, p2}
}

// Or predicate
func (p1 NEPred) Or(p2 Predicate) OrPred {
	return OrPred{p1, p2}
}

// Xor predicate
func (p1 NEPred) Xor(p2 Predicate) XorPred {
	return XorPred{p1, p2}
}

/*
// TODO(jonlawlor): other common comparisons
func (att Attribute) IN(v interface{}) INPred {
return
}
*/

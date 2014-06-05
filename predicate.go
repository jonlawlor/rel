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
	EvalFunc(e reflect.Type) func(t T) bool

	// Domain is the type of input that is required to evalute the predicate
	// this might have to be a recursive type instead of reflect.Type?
	Domain() []Attribute

	// infix boolean expressions
	And(p2 Predicate) AndPred
	Or(p2 Predicate) OrPred
	Xor(p2 Predicate) XorPred
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
func (p NotPred) String() string {
	return fmt.Sprintf("!(%v)", p.P)
}

// Domain is the type of input that is required to evalute the predicate
func (p NotPred) Domain() []Attribute {
	return p.P.Domain()
}

// Eval evalutes a predicate on an input tuple
func (p NotPred) EvalFunc(e reflect.Type) func(t T) bool {
	f := p.P.EvalFunc(e)
	return func(t T) bool { return !f(t) }
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
func (p AndPred) String() string {
	return fmt.Sprintf("(%v) && (%v)", p.P1, p.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p AndPred) Domain() []Attribute {
	return unionAttributes(p.P1.Domain(), p.P2.Domain())
}

// Eval evalutes a predicate on an input tuple
func (p AndPred) EvalFunc(e reflect.Type) func(t T) bool {
	f1 := p.P1.EvalFunc(e)
	f2 := p.P2.EvalFunc(e)
	return func(t T) bool { return f1(t) && f2(t) }
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
func (p OrPred) String() string {
	return fmt.Sprintf("(%v) || (%v)", p.P1, p.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p OrPred) Domain() []Attribute {
	return unionAttributes(p.P1.Domain(), p.P2.Domain())
}

// Eval evalutes a predicate on an input tuple
func (p OrPred) EvalFunc(e reflect.Type) func(t T) bool {

	f1 := p.P1.EvalFunc(e)
	f2 := p.P2.EvalFunc(e)
	return func(t T) bool { return f1(t) || f2(t) }
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
func (p XorPred) String() string {
	return fmt.Sprintf("(%v) != (%v)", p.P1, p.P2)
}

// Domain is the type of input that is required to evalute the predicate
func (p XorPred) Domain() []Attribute {
	return unionAttributes(p.P1.Domain(), p.P2.Domain())
}

// Eval evalutes a predicate on an input tuple
func (p XorPred) EvalFunc(e reflect.Type) func(t T) bool {
	f1 := p.P1.EvalFunc(e)
	f2 := p.P2.EvalFunc(e)
	return func(t T) bool {
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
	f interface{}
}

// String representation of AdHoc
func (p AdHoc) String() string {
	dom := p.Domain()
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
func (p AdHoc) Domain() []Attribute {
	return fieldNames(reflect.TypeOf(p.f).In(0))
}

// Eval evalutes a predicate on an input tuple
func (p AdHoc) EvalFunc(e1 reflect.Type) func(t T) bool {

	e2 := reflect.TypeOf(p.f).In(0)

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := fieldMap(e1, e2)
	pf := reflect.ValueOf(p.f)

	return func(tup1 T) bool {
		tup2 := reflect.Indirect(reflect.New(e2))
		rtup1 := reflect.ValueOf(tup1)
		for _, fm := range fMap {
			tupf2 := tup2.Field(fm.j)
			tupf2.Set(rtup1.Field(fm.i))
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

type EQPred struct {
	att []Attribute
	lit interface{}
}

// String representation of EQ
func (p EQPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v == %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v == %v", p.att[0], p.lit)
	}
}

// Equal to (==)
func (att1 Attribute) EQ(v interface{}) EQPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return EQPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return EQPred{att, v}
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p EQPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p EQPred) EvalFunc(e1 reflect.Type) func(t T) bool {

	// The only method defined on all interfaces is equal & not equal.

	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() == rtup1.FieldByName(att2).Interface()
		}
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() == p.lit
		}
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

type LTPred struct {
	att []Attribute
	lit interface{}
}

// String representation of LT
func (p LTPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v < %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v < %v", p.att[0], p.lit)
	}
}

// Less than (<)
func (att1 Attribute) LT(v interface{}) LTPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return LTPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return LTPred{att, v}
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p LTPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p LTPred) EvalFunc(e1 reflect.Type) func(t T) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
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
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) < p.lit.(int)
			case string:
				return f1.(string) < p.lit.(string)
			case uint8:
				return f1.(uint8) < p.lit.(uint8)
			case uint16:
				return f1.(uint16) < p.lit.(uint16)
			case uint32:
				return f1.(uint32) < p.lit.(uint32)
			case uint64:
				return f1.(uint64) < p.lit.(uint64)
			case int8:
				return f1.(int8) < p.lit.(int8)
			case int16:
				return f1.(int16) < p.lit.(int16)
			case int32:
				return f1.(int32) < p.lit.(int32)
			case int64:
				return f1.(int64) < p.lit.(int64)
			case float32:
				return f1.(float32) < p.lit.(float32)
			case float64:
				return f1.(float64) < p.lit.(float64)
			}
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

type LEPred struct {
	att []Attribute
	lit interface{}
}

// Less than or equal to (<=)
func (att1 Attribute) LE(v interface{}) LEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return LEPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return LEPred{att, v}
	}
}

// String representation of LE
func (p LEPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v <= %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v <= %v", p.att[0], p.lit)
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p LEPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p LEPred) EvalFunc(e1 reflect.Type) func(t T) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
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
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) <= p.lit.(int)
			case string:
				return f1.(string) <= p.lit.(string)
			case uint8:
				return f1.(uint8) <= p.lit.(uint8)
			case uint16:
				return f1.(uint16) <= p.lit.(uint16)
			case uint32:
				return f1.(uint32) <= p.lit.(uint32)
			case uint64:
				return f1.(uint64) <= p.lit.(uint64)
			case int8:
				return f1.(int8) <= p.lit.(int8)
			case int16:
				return f1.(int16) <= p.lit.(int16)
			case int32:
				return f1.(int32) <= p.lit.(int32)
			case int64:
				return f1.(int64) <= p.lit.(int64)
			case float32:
				return f1.(float32) <= p.lit.(float32)
			case float64:
				return f1.(float64) <= p.lit.(float64)
			}
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

type GTPred struct {
	att []Attribute
	lit interface{}
}

// String representation of GT
func (p GTPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v > %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v > %v", p.att[0], p.lit)
	}
}

// Greater than (>)
func (att1 Attribute) GT(v interface{}) GTPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return GTPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return GTPred{att, v}
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p GTPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p GTPred) EvalFunc(e1 reflect.Type) func(t T) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
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
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) > p.lit.(int)
			case string:
				return f1.(string) > p.lit.(string)
			case uint8:
				return f1.(uint8) > p.lit.(uint8)
			case uint16:
				return f1.(uint16) > p.lit.(uint16)
			case uint32:
				return f1.(uint32) > p.lit.(uint32)
			case uint64:
				return f1.(uint64) > p.lit.(uint64)
			case int8:
				return f1.(int8) > p.lit.(int8)
			case int16:
				return f1.(int16) > p.lit.(int16)
			case int32:
				return f1.(int32) > p.lit.(int32)
			case int64:
				return f1.(int64) > p.lit.(int64)
			case float32:
				return f1.(float32) > p.lit.(float32)
			case float64:
				return f1.(float64) > p.lit.(float64)
			}
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

type GEPred struct {
	att []Attribute
	lit interface{}
}

// String representation of EQ
func (p GEPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v >= %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v >= %v", p.att[0], p.lit)
	}
}

// Greater than or equal to (>=)
func (att1 Attribute) GE(v interface{}) GEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return GEPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return GEPred{att, v}
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p GEPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p GEPred) EvalFunc(e1 reflect.Type) func(t T) bool {
	// e1 is currently unused

	// Less than is only defined on numeric and string types
	// TODO(jonlawlor): this is hideous!
	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
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
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			f1 := rtup1.FieldByName(att1).Interface()
			switch f1.(type) {
			default:
				// I am _REALLY_ unsure this is the desired behavior
				return false
			case int:
				return f1.(int) >= p.lit.(int)
			case string:
				return f1.(string) >= p.lit.(string)
			case uint8:
				return f1.(uint8) >= p.lit.(uint8)
			case uint16:
				return f1.(uint16) >= p.lit.(uint16)
			case uint32:
				return f1.(uint32) >= p.lit.(uint32)
			case uint64:
				return f1.(uint64) >= p.lit.(uint64)
			case int8:
				return f1.(int8) >= p.lit.(int8)
			case int16:
				return f1.(int16) >= p.lit.(int16)
			case int32:
				return f1.(int32) >= p.lit.(int32)
			case int64:
				return f1.(int64) >= p.lit.(int64)
			case float32:
				return f1.(float32) >= p.lit.(float32)
			case float64:
				return f1.(float64) >= p.lit.(float64)
			}
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

type NEPred struct {
	att []Attribute
	lit interface{}
}

// String representation of EQ
func (p NEPred) String() string {
	if len(p.att) == 2 {
		return fmt.Sprintf("%v != %v", p.att[0], p.att[1])
	} else {
		return fmt.Sprintf("%v != %v", p.att[0], p.lit)
	}
}

// Not equal to (!=)
func (att1 Attribute) NE(v interface{}) NEPred {
	if att2, ok := v.(Attribute); ok {
		att := make([]Attribute, 2)
		att[0] = att1
		att[1] = att2
		return NEPred{att, reflect.Value{}}
	} else { // v is a literal, we'll need runtime reflection
		att := make([]Attribute, 1)
		att[0] = att1
		return NEPred{att, v}
	}
}

// Domain is the type of input that is required to evalute the predicate
func (p NEPred) Domain() []Attribute {
	return p.att
}

// Eval evalutes a predicate on an input tuple
func (p NEPred) EvalFunc(e1 reflect.Type) func(t T) bool {

	// The only method defined on all interfaces is equal & not equal.

	if len(p.att) == 2 {
		att1 := string(p.att[0])
		att2 := string(p.att[1])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() != rtup1.FieldByName(att2).Interface()
		}
	} else { // the second element is a literal
		att1 := string(p.att[0])
		return func(tup1 T) bool {
			rtup1 := reflect.ValueOf(tup1)
			return rtup1.FieldByName(att1).Interface() != p.lit
		}
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
// other common comparisons
func (att Attribute) IN(v interface{}) INPred {
return
}
*/

// predicate defines logical predicates used in relation's restrict

// we might want to move this into a different package so that compound
// predicates like "And" and "Or" will have a clear meaning.

package rel

import "reflect"

// Predicate is the type of func that takes a tuple and returns bool
// and is used for restrict.  It should always be a func with input of a
// subdomain of the relation, with one bool output.
type Predicate interface {
	// Eval evalutes a predicate on an input tuple
	Eval(t reflect.Value) bool

	// Domain is the type of input that is required to evalute the predicate
	Domain() reflect.Type
}

// AdHoc is a Predicate that can implement any function on a tuple.
// The rewrite engine will be able to infer which attributes it requires to be
// evaluated, but nothing beyond that, which will prevent it from being moved
// into source queries in e.g. sql.  For those kind of predicates, non AdHoc
// predicates will be required.
type AdHoc struct {
	// f is the function which takes a tuple and returns a boolean indicating
	// that the tuple passes the predicate
	f interface{}
}

// Eval evalutes a predicate on an input tuple
func (p AdHoc) Eval(t reflect.Value) bool {
	parm := make([]reflect.Value, 1)
	parm[0] = t
	// TODO(jonlawlor): this creates significant overhead.  We can evaluate
	// some of this expression just a single time for each predicate rather
	// than once per tuple.
	pf := reflect.ValueOf(p.f)
	b := pf.Call(parm)
	return b[0].Interface().(bool)
}

// Domain is the type of input that is required to evalute the predicate
func (p AdHoc) Domain() reflect.Type {
	f := reflect.TypeOf(p.f)
	return f.In(0)
}

package rel

import (
	"fmt"
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// type for error testing only.  It produces an error if tuples is called, and
// does not allow any query rewrite.
type errorRel struct {
	// the type of the tuples
	zero interface{}

	card int

	err error
}

// TupleChan sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *errorRel) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := att.EnsureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	go func(res reflect.Value) {
		for i := 0; i < r.card; i++ {
			// note: these won't be distinct.
			res.Send(reflect.ValueOf(r.zero))
		}
		r.err = fmt.Errorf("testing error")
		res.Close()
	}(chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *errorRel) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *errorRel) CKeys() att.CandKeys {
	return att.CandKeys{}
}

// GoString returns a text representation of the Relation
func (r *errorRel) GoString() string {
	return "error{" + HeadingString(r) + "}"
}

// String returns a text representation of the Relation
func (r *errorRel) String() string {
	return "error{" + HeadingString(r) + "}"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *errorRel) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *errorRel) Restrict(p att.Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *errorRel) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *errorRel) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
//
func (r1 *errorRel) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *errorRel) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *errorRel) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *errorRel) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *errorRel) Err() error {
	return r1.err
}

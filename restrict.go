// restrict implements a restrict expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
	"runtime"
	"sync"
)

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
type restrictExpr struct {
	// the input relation
	source1 Relation

	// the restriction predicate
	p att.Predicate

	err error
}

func (r *restrictExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	// transform the channel of tuples from the relation
	mc := runtime.GOMAXPROCS(-1)

	z1 := r.source1.Zero()
	e1 := reflect.TypeOf(z1)

	predFunc := r.p.EvalFunc(e1)

	body1 := make(chan interface{})
	bcancel := r.source1.Tuples(body1)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan<- interface{}) {
		wg.Wait()
		// if we've been cancelled, send it up to the source
		select {
		case <-cancel:
			close(bcancel)
		default:
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			close(res)
		}
	}(t)

	for i := 0; i < mc; i++ {
		go func(body <-chan interface{}, res chan<- interface{}, p att.Predicate) {
		Loop:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop
					}
					// call the predicate with the new tuple to determine if it should
					// go into the results
					if predFunc(tup1) {
						select {
						case res <- tup1:
						case <-cancel:
							break Loop
						}
					}
				case <-cancel:
					break Loop
				}
			}
			wg.Done()
		}(body1, t, r.p)
	}
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *restrictExpr) Zero() interface{} {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *restrictExpr) CKeys() att.CandKeys {
	return r.source1.CKeys()
}

// GoString returns a text representation of the Relation
func (r *restrictExpr) GoString() string {
	return r.source1.GoString() + ".Restrict(" + r.p.String() + ")"
}

// String returns a text representation of the Relation
func (r *restrictExpr) String() string {
	return "σ{" + r.p.String() + "}(" + r.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *restrictExpr) Project(z2 interface{}) Relation {
	att2 := att.FieldNames(reflect.TypeOf(z2))
	if att.IsSubDomain(r1.p.Domain(), att2) { // the predicate's attributes exist after project
		return NewRestrict(r1.source1.Project(z2), r1.p)
	} else {
		return NewProject(r1, z2)
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *restrictExpr) Restrict(p att.Predicate) Relation {
	// try reversing the order, which may allow some lower degree restrictions
	// to pass through
	return NewRestrict(r1.source1.Restrict(p), r1.p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *restrictExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *restrictExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *restrictExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *restrictExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *restrictExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *restrictExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *restrictExpr) Err() error {
	return r1.err
}

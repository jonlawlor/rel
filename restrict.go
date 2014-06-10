// restrict implements a restrict expression in relational algebra

package rel

import (
	"reflect"
	"runtime"
	"sync"
)

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
type RestrictExpr struct {
	// the input relation
	source Relation

	// the restriction predicate
	p Predicate

	err error
}

func (r *RestrictExpr) Tuples(t chan<- T) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	// transform the channel of tuples from the relation
	mc := runtime.GOMAXPROCS(-1)

	z1 := r.source.Zero()
	e1 := reflect.TypeOf(z1)

	predFunc := r.p.EvalFunc(e1)

	body1 := make(chan T)
	bcancel := r.source.Tuples(body1)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan<- T) {
		wg.Wait()
		// if we've been cancelled, send it up to the source
		select {
		case <-cancel:
			close(bcancel)
		default:
			if err := r.source.Err(); err != nil {
				r.err = err
			}
			close(res)
		}
	}(t)

	for i := 0; i < mc; i++ {
		go func(body <-chan T, res chan<- T, p Predicate) {
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
func (r *RestrictExpr) Zero() T {
	return r.source.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *RestrictExpr) CKeys() CandKeys {
	return r.source.CKeys()
}

// GoString returns a text representation of the Relation
func (r *RestrictExpr) GoString() string {
	return r.source.GoString() + ".Restrict(" + r.p.String() + ")"
}

// String returns a text representation of the Relation
func (r *RestrictExpr) String() string {
	return "σ{" + r.p.String() + "}(" + r.source.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *RestrictExpr) Project(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	}
	if isSubDomain(r1.p.Domain(), att2) { // the predicate's attributes exist after project
		return &RestrictExpr{r1.source.Project(z2), r1.p, nil}
	} else {
		return &ProjectExpr{r1, z2, nil}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *RestrictExpr) Restrict(p Predicate) Relation {
	if r1.Err() != nil {
		return r1
	}
	// TODO(jonlawlor): implement predicate combination

	// by reversing the order, this provides a way for AndPreds to pass through
	return &RestrictExpr{r1.source.Restrict(p), r1.p, nil}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *RestrictExpr) Rename(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RenameExpr{r1, z2, nil}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *RestrictExpr) Union(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &UnionExpr{r1, r2, nil}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *RestrictExpr) SetDiff(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &SetDiffExpr{r1, r2, nil}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *RestrictExpr) Join(r2 Relation, zero T) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	// TODO(jonlawlor): test to see if the restrict op can also be applied to
	// r2.
	return &JoinExpr{r1, r2, zero, nil}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *RestrictExpr) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &GroupByExpr{r1, t2, vt, gfcn, nil}
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *RestrictExpr) Map(mfcn func(from T) (to T), z2 T, ckeystr [][]string) Relation {
	if r1.Err() != nil {
		return r1
	}
	// determine the type of the returned tuples
	r := new(MapExpr)
	r.source1 = r1
	r.zero = z2
	r.fcn = mfcn
	if len(ckeystr) == 0 {
		// all relations have a candidate key of all of their attributes, or
		// a non zero subset if the relation is not dee or dum
		r.cKeys = defaultKeys(z2)
	} else {
		r.isDistinct = true
		// convert from [][]string to CandKeys
		r.cKeys = string2CandKeys(ckeystr)
	}
	return r
}

// Error returns an error encountered during construction or computation
func (r1 *RestrictExpr) Err() error {
	return r1.err
}

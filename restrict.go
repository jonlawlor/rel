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
}

func (r *RestrictExpr) Tuples(t chan T) {
	// transform the channel of tuples from the relation
	mc := runtime.GOMAXPROCS(-1)

	z1 := r.source.Zero()
	e1 := reflect.TypeOf(z1)

	predFunc := r.p.EvalFunc(e1)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan T) {
		wg.Wait()
		close(res)
	}(t)

	body1 := make(chan T)
	r.source.Tuples(body1)
	for i := 0; i < mc; i++ {
		go func(body, res chan T, p Predicate) {
			for tup1 := range body {
				// call the predicate with the new tuple to determine if it should
				// go into the results
				if predFunc(tup1) {
					res <- tup1
				}
			}
			wg.Done()
		}(body1, t, r.p)
	}
	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *RestrictExpr) Zero() T {
	return r.source.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *RestrictExpr) CKeys() CandKeys {
	return r.source.CKeys()
}

// text representation
const restrictSymbol = "σ"

// GoString returns a text representation of the Relation
func (r *RestrictExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *RestrictExpr) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *RestrictExpr) Project(z2 T) Relation {
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	}
	if isSubDomain(r1.p.Domain(), att2) { // the predicate's attributes exist after project
		return &RestrictExpr{r1.source.Project(z2), r1.p}
	} else {
		return &ProjectExpr{r1, z2}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *RestrictExpr) Restrict(p Predicate) Relation {
	// TODO(jonlawlor): implement predicate combination

	// by reversing the order, this provides a way for AndPreds to pass through
	return &RestrictExpr{r1.source.Restrict(p), r1.p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *RestrictExpr) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *RestrictExpr) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *RestrictExpr) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *RestrictExpr) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *RestrictExpr) GroupBy(t2, vt T, gfcn func(chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

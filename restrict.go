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

func (r RestrictExpr) Tuples(t chan T) {
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
func (r RestrictExpr) Zero() T {
	return r.source.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r RestrictExpr) CKeys() CandKeys {
	return r.source.CKeys()
}

// text representation
const restrictSymbol = "σ"

// GoString returns a text representation of the Relation
func (r RestrictExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r RestrictExpr) String() string {
	return stringTabTable(r)
}

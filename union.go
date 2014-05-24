// union implements a union expression in relational algebra

package rel

import "sync"

//UnionExpr represents a union expression in relational algebra.  This is one
// of the relational operations which consumes memory.
type UnionExpr struct {
	source1 Relation
	source2 Relation
}

func (r UnionExpr) Tuples(t chan T) {
	// transform the channel of tuples from the relation
	var mu sync.Mutex
	m := make(map[interface{}]struct{})

	body1 := make(chan T)
	body2 := make(chan T)
	go r.source1.Tuples(body1)
	go r.source2.Tuples(body2)

	done := make(chan struct{})
	// function to handle closing of the results channel
	go func(res chan T) {
		// one for each body.  We could replace this with a pool of workers
		<-done
		<-done
		close(res)
	}(t)

	combine := func(body chan T, res chan T) {
		for tup := range body {
			mu.Lock()
			if _, dup := m[tup]; !dup {
				m[tup] = struct{}{}
				mu.Unlock()
				res <- tup
			} else {
				mu.Unlock()
			}
		}
		done <- struct{}{}
		return
	}
	go combine(body1, t)
	go combine(body2, t)
	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r UnionExpr) Zero() T {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r UnionExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// text representation
const unionSymbol = "+"

// GoString returns a text representation of the Relation
func (r UnionExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r UnionExpr) String() string {
	return stringTabTable(r)
}

/* needs to be rewritten
func (r1 *Simple) UnionExpr(r2 *Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	// turn the first relation into a map and then add on the values from
	// the second one, then return the keys as a new relation

	// for some reason the map requires this to use an Interface() call.
	// maybe there is a better way?

}
*/

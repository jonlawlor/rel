//setdiff implements a set difference expression in relational algebra

package rel

// SetDiffExpr implements a set difference in relational algebra
// This is one of the operations which consumes memory.  In addition, no values
// can be sent before all values from the second source are consumed.
type SetDiffExpr struct {
	source1 Relation
	source2 Relation
}

func (r SetDiffExpr) Tuples(t chan T) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	m := make(map[interface{}]struct{})
	// setdiff is unique in that it has to immediately consume all of the
	// values from the second relation in order to send any values in the
	// first one.  All other relational operations can be done lazily, this
	// one can only be done half-lazy.
	// with some indexing this is avoidable, though.

	// get the values out of the source relations
	body1 := make(chan T)
	body2 := make(chan T)
	go r.source1.Tuples(body1)
	go r.source2.Tuples(body2)

	go func(b1, b2, res chan T) {
		for tup := range b2 {
			m[tup] = struct{}{}
		}
		for tup := range b1 {
			if _, rem := m[tup]; !rem {
				res <- tup
			}
		}
		close(res)
	}(body1, body2, t)
}

// Zero returns the zero value of the relation (a blank tuple)
func (r SetDiffExpr) Zero() T {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r SetDiffExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// text representation
const setDiffSymbol = "-"

// GoString returns a text representation of the Relation
func (r SetDiffExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r SetDiffExpr) String() string {
	return stringTabTable(r)
}

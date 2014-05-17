//setdiff implements a set difference expression in relational algebra

package rel

type SetDiffExpr struct {
	source1 Relation
	source2 Relation
}

/* Needs to be rewritten
// setdiff returns the set difference of the two relations
func (r1 *Simple) SetDiffExpr(r2 *Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	m := make(map[interface{}]struct{})
	// setdiff is unique in that it has to immediately consume all of the
	// values from the second relation in order to send any values in the
	// first one.  All other relational operations can be done lazily, this
	// one can only be done half-lazy.
	// with some indexing this is avoidable.

	// second set of tups
	body2 := make(chan T)
	r2.Tuples(body2)

	res := make(chan T)

	go func(b1, b2 chan T) {
		for tup := range b2 {
			m[tup] = struct{}{}
		}
		for tup := range b1 {
			if _, rem := m[tup]; !rem {
				res <- tup
			}
		}
		close(res)
	}(r1.body, body2)

	r1.body = res
}
*/

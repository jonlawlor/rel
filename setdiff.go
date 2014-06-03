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

	mem := make(map[interface{}]struct{})
	// setdiff is unique in that it has to immediately consume all of the
	// values from the second relation in order to send any values in the
	// first one.  All other relational operations can be done lazily, this
	// one can only be done half-lazy.
	// with some indexing this is avoidable, though.
	// In addition, we could start pulling values from the first relation as
	// well if we were willing to later go through them.  However, this will
	// always use more memory, so we can leave it up to the caller.

	// get the values out of the source relations
	body1 := make(chan T)
	body2 := make(chan T)
	go r.source1.Tuples(body1)
	go r.source2.Tuples(body2)

	go func(b1, b2, res chan T) {
		for tup := range b2 {
			mem[tup] = struct{}{}
		}
		for tup := range b1 {
			if _, rem := mem[tup]; !rem {
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
const setDiffSymbol = "−"

// GoString returns a text representation of the Relation
func (r SetDiffExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r SetDiffExpr) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 SetDiffExpr) Project(z2 T) Relation {
	return ProjectExpr{r1, z2}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r SetDiffExpr) Restrict(p Predicate) Relation {
	return RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 SetDiffExpr) Rename(z2 T) Relation {
	return RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 SetDiffExpr) Union(r2 Relation) Relation {
	return UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 SetDiffExpr) SetDiff(r2 Relation) Relation {
	return SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 SetDiffExpr) Join(r2 Relation, zero T) Relation {
	return JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 SetDiffExpr) GroupBy(t2, vt T, gfcn func(chan T) T) Relation {
	return GroupByExpr{r1, t2, vt, gfcn}
}

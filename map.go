// map implements a set mapping expression in relational algebra

package rel

// projection is a type that represents a project operation
type MapExpr struct {
	// the input relation
	source1 Relation

	// zero is the resulting relation tuple type
	zero T

	// the function that maps from source tuple type to result tuple type
	fcn func(T) T

	// set of candidate keys
	cKeys CandKeys

	// sourceDistinct indicates if the function results in distinct tuples or
	// if a distinct has to be performed afterwards
	isDistinct bool

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *MapExpr) Tuples(t chan<- T) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	body1 := make(chan T)
	bcancel := r.source1.Tuples(body1)

	if r.isDistinct {
		// assign fields from the old relation to fields in the new
		// TODO(jonlawlor): add parallelism here
		go func(body <-chan T, res chan<- T) {
		Loop:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop
					}
					select {
					// set the field in the new tuple to the value from the old one
					case t <- r.fcn(tup1):
					case <-cancel:
						close(bcancel)
						return
					}
				case <-cancel:
					close(bcancel)
					return
				}
			}
			close(t)
		}(body1, t)

		return cancel
	}
	go func(body <-chan T, res chan<- T) {
		m := map[interface{}]struct{}{}
	Loop:
		for {
			select {
			case tup1, ok := <-body:
				if !ok {
					break Loop
				}
				tup2 := r.fcn(tup1)
				// set the field in the new tuple to the value from the old one
				if _, isdup := m[tup2]; !isdup {
					m[tup2] = struct{}{}
					select {
					case t <- tup2:
					case <-cancel:
						close(bcancel)
						return
					}
				}
			case <-cancel:
				close(bcancel)
				return
			}
		}
		close(t)
	}(body1, t)

	return cancel

}

// Zero returns the zero value of the relation (a blank tuple)
func (r *MapExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *MapExpr) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *MapExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *MapExpr) String() string {
	return r.source1.String() + ".Map({" + HeadingString(r.source1) + "}->{" + HeadingString(r) + "})"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *MapExpr) Project(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &ProjectExpr{r1, z2, nil}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *MapExpr) Restrict(p Predicate) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RestrictExpr{r1, p, nil}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *MapExpr) Rename(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RenameExpr{r1, z2, nil}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *MapExpr) Union(r2 Relation) Relation {
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
func (r1 *MapExpr) SetDiff(r2 Relation) Relation {
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
func (r1 *MapExpr) Join(r2 Relation, zero T) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &JoinExpr{r1, r2, zero, nil}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *MapExpr) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &GroupByExpr{r1, t2, vt, gfcn, nil}
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *MapExpr) Map(mfcn func(from T) (to T), z2 T, ckeystr [][]string) Relation {
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
func (r1 *MapExpr) Err() error {
	return r1.err
}

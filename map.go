// map implements a set mapping expression in relational algebra

package rel

import "github.com/jonlawlor/rel/att"

// projection is a type that represents a project operation
type mapExpr struct {
	// the input relation
	source1 Relation

	// zero is the resulting relation tuple type
	zero interface{}

	// the function that maps from source tuple type to result tuple type
	fcn func(interface{}) interface{}

	// set of candidate keys
	cKeys att.CandKeys

	// sourceDistinct indicates if the function results in distinct tuples or
	// if a distinct has to be performed afterwards
	isDistinct bool

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *mapExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	body1 := make(chan interface{})
	bcancel := r.source1.Tuples(body1)

	if r.isDistinct {
		// assign fields from the old relation to fields in the new
		// TODO(jonlawlor): add parallelism here
		go func(body <-chan interface{}, res chan<- interface{}) {
		Loop:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop
					}
					select {
					// set the field in the new tuple to the value from the old one
					case res <- r.fcn(tup1):
					case <-cancel:
						close(bcancel)
						return
					}
				case <-cancel:
					close(bcancel)
					return
				}
			}
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			close(res)
		}(body1, t)

		return cancel
	}
	go func(body <-chan interface{}, res chan<- interface{}) {
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
		if err := r.source1.Err(); err != nil {
			r.err = err
		}
		close(t)
	}(body1, t)

	return cancel

}

// Zero returns the zero value of the relation (a blank tuple)
func (r *mapExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *mapExpr) CKeys() att.CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *mapExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *mapExpr) String() string {
	return r.source1.String() + ".Map({" + HeadingString(r.source1) + "}->{" + HeadingString(r) + "})"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *mapExpr) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *mapExpr) Restrict(p att.Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *mapExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *mapExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *mapExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *mapExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *mapExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *mapExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *mapExpr) Err() error {
	return r1.err
}

package rel

import "fmt"

// type for error testing only.  It produces an error if tuples is called, and
// does not allow any query rewrite.
type errorRel struct {
	// the type of the tuples
	zero T

	card int

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *errorRel) Tuples(t chan<- T) chan<- struct{} {
	go func(res chan<- T) {
		for i := 0; i < r.card; i++ {
			t <- r.zero
		}
		r.err = fmt.Errorf("testing error")
		close(t)
	}(t)
	return make(chan struct{})
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *errorRel) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *errorRel) CKeys() CandKeys {
	return CandKeys{}
}

// GoString returns a text representation of the Relation
func (r *errorRel) GoString() string {
	return "error{" + HeadingString(r) + "}"
}

// String returns a text representation of the Relation
func (r *errorRel) String() string {
	return "error{" + HeadingString(r) + "}"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *errorRel) Project(z2 T) Relation {
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
func (r1 *errorRel) Restrict(p Predicate) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RestrictExpr{r1, p, nil}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *errorRel) Rename(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RenameExpr{r1, z2, nil}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *errorRel) Union(r2 Relation) Relation {
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
func (r1 *errorRel) SetDiff(r2 Relation) Relation {
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
func (r1 *errorRel) Join(r2 Relation, zero T) Relation {
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
func (r1 *errorRel) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &GroupByExpr{r1, t2, vt, gfcn, nil}
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *errorRel) Map(mfcn func(from T) (to T), z2 T, ckeystr [][]string) Relation {
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
func (r1 *errorRel) Err() error {
	return r1.err
}

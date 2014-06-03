// Chan is a relation with underlying data stored in a channel.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a chan of structs.
//

package rel

import "reflect"

// Chan is an implementation of Relation using a channel
type Chan struct {
	// the channel of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T

	// sourceDistinct indicates if the source chan was already distinct or if a
	// distinct has to be performed when sending tuples
	sourceDistinct bool
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r Chan) Tuples(t chan T) {
	if r.sourceDistinct {
		go func(rbody reflect.Value, res chan T) {
			for {
				rtup, ok := rbody.Recv()
				if !ok {
					break
				}
				res <- rtup.Interface()
			}
			close(res)
		}(r.rbody, t)
		return
	}
	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[T]struct{}{}
	go func(rbody reflect.Value, res chan T) {
		for {
			rtup, ok := rbody.Recv()
			if !ok {
				break
			}
			tup := rtup.Interface()
			if _, dup := mem[tup]; !dup {
				res <- tup
				mem[tup] = struct{}{}
			}
		}
		close(res)
	}(r.rbody, t)
	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r Chan) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r Chan) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r Chan) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r Chan) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 Chan) Project(z2 T) Relation {
	return ProjectExpr{r1, z2}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r Chan) Restrict(p Predicate) Relation {
	return RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 Chan) Rename(z2 T) Relation {
	return RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 Chan) Union(r2 Relation) Relation {
	return UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 Chan) SetDiff(r2 Relation) Relation {
	return SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 Chan) Join(r2 Relation, zero T) Relation {
	return JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 Chan) GroupBy(t2, vt T, gfcn func(chan T) T) Relation {
	return GroupByExpr{r1, t2, vt, gfcn}
}

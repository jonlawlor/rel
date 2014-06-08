// Map is a relation with underlying data stored in a map.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a map with key as structs (but no values!!!).
//
// One nice thing is that Maps will never have duplicate keys.

package rel

import (
	"reflect"
)

// Map is an implementation of Relation using a map
type Map struct {

	// the map of tuples in the relation, with tuples in the key
	rbody reflect.Value // should always hold a map

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T
}

// Tuples sends each tuple in the relation to a channel
func (r *Map) Tuples(t chan<- T) chan<- struct{} {
	cancel := make(chan struct{})
	go func() {
		for _, rtup := range r.rbody.MapKeys() {
			select {
			case t <- rtup.Interface().(T):
			case <-cancel:
				break
			}
		}
		close(t)
	}()
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *Map) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *Map) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *Map) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *Map) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *Map) Project(z2 T) Relation {
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &ProjectExpr{r1, z2}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r *Map) Restrict(p Predicate) Relation {
	return &RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *Map) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *Map) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *Map) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *Map) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *Map) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

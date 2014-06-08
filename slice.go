// slice implements one of the possible ways of creating a new relation from
// scratch, specifically, with a slice of structs

package rel

import (
	"reflect"
)

// slice represents a relation that came from a slice of a struct
type Slice struct {
	// the slice of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T

	// sourceDistinct indicates if the source slice was already distinct or if
	// a distinct has to be performed when sending tuples
	sourceDistinct bool
}

// Tuples sends each tuple in the relation to a channel
// and when it is finished it closes the input channel.
func (r *Slice) Tuples(t chan<- T) chan<- struct{} {
	cancel := make(chan struct{})
	if r.sourceDistinct {
		go func(rbody reflect.Value, res chan<- T) {
			for i := 0; i < rbody.Len(); i++ {
				select {
				case res <- rbody.Index(i).Interface().(T):
				case <-cancel:
					break
				}
			}
			close(res)
		}(r.rbody, t)
		return cancel
	}

	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[T]struct{}{}
	go func(rbody reflect.Value, res chan<- T) {
		for i := 0; i < rbody.Len(); i++ {
			tup := rbody.Index(i).Interface().(T)
			if _, dup := mem[tup]; !dup {
				select {
				case res <- tup:
				case <-cancel:
					break
				}
				mem[tup] = struct{}{}
			}
		}
		close(res)
	}(r.rbody, t)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *Slice) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *Slice) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *Slice) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *Slice) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *Slice) Project(z2 T) Relation {
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
func (r *Slice) Restrict(p Predicate) Relation {
	return &RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *Slice) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *Slice) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *Slice) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *Slice) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *Slice) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

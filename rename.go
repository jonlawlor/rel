// rename implements a rename expression in relational algebra
// it is called renaming instead of rename because there aren't any good
// synonyms of rename.

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

type renameExpr struct {
	// the input relation
	source1 Relation

	// the new names for the same positions
	zero interface{}

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *renameExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	// TODO(jonlawlor) add a check that the second interface's type is
	// the same as the first, except that it has different names for
	// the same fields.

	// first figure out if the tuple types of the relation and rename
	// are equal.  If so, convert the tuples to the (possibly new)
	// type and then return the new relation.
	z1 := reflect.TypeOf(r.source1.Zero())
	z2 := reflect.TypeOf(r.zero)

	body1 := make(chan interface{})
	bcancel := r.source1.Tuples(body1)
	// assign the values of the original to the new names in the same
	// locations
	n := z2.NumField()

	go func(body <-chan interface{}, res chan<- interface{}) {
		if z1.AssignableTo(z2) {
		Loop1:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop1
					}
					select {
					case res <- tup1:
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
			return
		}
	Loop2:
		for {
			select {
			case tup1, ok := <-body:
				if !ok {
					break Loop2
				}
				tup2 := reflect.Indirect(reflect.New(z2))
				rtup1 := reflect.ValueOf(tup1)
				for i := 0; i < n; i++ {
					tupf2 := tup2.Field(i)
					tupf2.Set(rtup1.Field(i))
				}
				select {
				case res <- tup2.Interface():
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

// Zero returns the zero value of the relation (a blank tuple)
func (r *renameExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *renameExpr) CKeys() att.CandKeys {
	z2 := reflect.TypeOf(r.zero)

	// figure out the new names
	names2 := att.FieldNames(z2)

	// create a map from the old names to the new names if there is any
	// difference between them
	nameMap := make(map[att.Attribute]att.Attribute)
	for i, att := range Heading(r.source1) {
		nameMap[att] = names2[i]
	}

	cKeys1 := r.source1.CKeys()
	cKeys2 := make(att.CandKeys, len(cKeys1))
	// for each of the candidate keys, rename any keys from the old names to
	// the new ones
	for i := range cKeys1 {
		cKeys2[i] = make([]att.Attribute, len(cKeys1[i]))
		for j, key := range cKeys1[i] {
			cKeys2[i][j] = nameMap[key]
		}
	}
	// order the keys
	att.OrderCandidateKeys(cKeys2)
	return cKeys2
}

// GoString returns a text representation of the Relation
func (r *renameExpr) GoString() string {
	return r.source1.GoString() + ".Rename(" + HeadingString(r) + ")"
}

// String returns a text representation of the Relation
func (r *renameExpr) String() string {
	return "ρ{" + HeadingString(r) + "}/{" + HeadingString(r.source1) + "}(" + r.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *renameExpr) Project(z2 interface{}) Relation {
	if r1.Err() != nil {
		return r1
	}
	// special case: we can't do a rewrite because the order could be different
	return &projectExpr{r1, z2, nil}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *renameExpr) Restrict(p att.Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *renameExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1.source1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *renameExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *renameExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *renameExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *renameExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *renameExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *renameExpr) Err() error {
	return r1.err
}

// rename implements a rename expression in relational algebra
// it is called renaming instead of rename because there aren't any good
// synonyms of rename.

package rel

import "reflect"

type RenameExpr struct {
	// the input relation
	source Relation

	// the new names for the same positions
	zero T
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *RenameExpr) Tuples(t chan<- T) {
	// TODO(jonlawlor) add a check that the second interface's type is
	// the same as the first, except that it has different names for
	// the same fields.

	// first figure out if the tuple types of the relation and rename
	// are equal.  If so, convert the tuples to the (possibly new)
	// type and then return the new relation.
	z1 := reflect.TypeOf(r.source.Zero())
	z2 := reflect.TypeOf(r.zero)

	body1 := make(chan T)
	r.source.Tuples(body1)
	// assign the values of the original to the new names in the same
	// locations
	n := z2.NumField()

	go func(body <-chan T, res chan<- T) {
		if z1.AssignableTo(z2) {
			for tup1 := range body {
				res <- tup1
			}
		} else {
			for tup1 := range body {
				tup2 := reflect.Indirect(reflect.New(z2))
				rtup1 := reflect.ValueOf(tup1)
				for i := 0; i < n; i++ {
					tupf2 := tup2.Field(i)
					tupf2.Set(rtup1.Field(i))
				}
				res <- tup2.Interface()
			}
		}
		close(res)
	}(body1, t)
	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *RenameExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *RenameExpr) CKeys() CandKeys {
	z2 := reflect.TypeOf(r.zero)

	// figure out the new names
	names2 := fieldNames(z2)

	// create a map from the old names to the new names if there is any
	// difference between them
	nameMap := make(map[Attribute]Attribute)
	for i, att := range Heading(r.source) {
		nameMap[att] = names2[i]
	}

	cKeys1 := r.source.CKeys()
	cKeys2 := make(CandKeys, len(cKeys1))
	// for each of the candidate keys, rename any keys from the old names to
	// the new ones
	for i := range cKeys1 {
		cKeys2[i] = make([]Attribute, len(cKeys1[i]))
		for j, key := range cKeys1[i] {
			cKeys2[i][j] = nameMap[key]
		}
	}

	return cKeys2
}

// GoString returns a text representation of the Relation
func (r *RenameExpr) GoString() string {
	return r.source.GoString() + ".Rename(" + HeadingString(r) + ")"
}

// String returns a text representation of the Relation
func (r *RenameExpr) String() string {
	return "ρ{" + HeadingString(r) + "}/{" + HeadingString(r.source) + "}(" + r.source.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *RenameExpr) Project(z2 T) Relation {
	return &ProjectExpr{r1, z2}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r *RenameExpr) Restrict(p Predicate) Relation {
	return &RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *RenameExpr) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *RenameExpr) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *RenameExpr) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *RenameExpr) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *RenameExpr) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

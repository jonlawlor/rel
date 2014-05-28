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
func (r RenameExpr) Tuples(t chan T) {
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

	go func(body, res chan T) {
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
func (r RenameExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r RenameExpr) CKeys() CandKeys {
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

// text representation

const renameSymbol = "ρ"

// GoString returns a text representation of the Relation
func (r RenameExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r RenameExpr) String() string {
	return stringTabTable(r)
}

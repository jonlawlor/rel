// rename implements a rename expression in relational algebra
// it is called renaming instead of rename because there aren't any good
// synonyms of rename.

package rel

type RenameExpr struct {
	// the input relation
	source Relation

	// the new names for the same positions
	zero T
}

/* needs a rewrite
// rename operation
// the way this is done has to do a rename in place, so at this point
// the order of the fields becomes significant.  There will only be a
// single input, which should line up with the order and types of the
// fields in the original data.
// The advantage of this syntax in go is that the rename can't express
// duplicate attributes, and also the renamed tuple is the new type
// used in type assertions on resulting values.
func (r *Simple) Rename(t2 interface{}) {
	// TODO(jonlawlor) add a check that the second interface's type is
	// the same as the first, except that it has different names for
	// the same fields.

	// first figure out if the tuple types of the relation and rename
	// are equal.  If so, convert the tuples to the (possibly new)
	// type and then return the new relation.
	z1 := reflect.TypeOf(r.zero)
	z2 := reflect.TypeOf(t2)
	if z1.AssignableTo(z2) {
		// do nothing
		return
	}

	body2 := make(chan T)
	// assign the values of the original to the new names in the same
	// locations
	n := reflect.ValueOf(z2).NumField()

	go func(body chan T) {
		for tup1 := range r1.Body {
			tup2 := reflect.Indirect(reflect.New(e2))
			rtup1 := reflect.ValueOf(tup1)
			for i := 0; i < n; i++ {
				tupf2 := tup2.Field(i)
				tupf2.Set(rtup1.Field(i))
			}
			body2 <- tup2.Interface()
		}
		close(body2)
	}(r.body)
	r.body = body2

	// figure out the new names
	names2 := make([]string, n)
	for i := 0; i < n; i++ {
		f := e2.Field(i)
		names2[i] = f.Name
	}

	// create a map from the old names to the new names if there is
	// any difference between them
	nameMap := make(map[string]string)
	for i, att := range r.heading {
		nameMap[att.Name] = names2[i]
	}

	// for each of the candidate keys, rename any keys from the old
	// names to the new ones
	for i := 0; i < len(r.cKeys); i++ {
		for j, key := range r.cKeys[i] {
			r.cKeys[i][j] = nameMap[key]
		}
	}

	// change the heading
	for i := 0; i < len(r.heading); i++ {
		r.heading[i].Name = names2[i]
	}

}
*/

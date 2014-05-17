package rel

// projection is a type that represents a project operation

type ProjectExpr struct {
	// the input relation
	source Relation

	// the new tuple type
	zero T
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *ProjectExpr) Tuples(t chan T) {
	// transform the channel of tuples from the relation
	z1 := r1.Zero()

	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e1 := reflect.TypeOf(r1.Zero())
	e2 := reflect.TypeOf(z2)

	if e1.AssignableTo(e2) {
		// nothing to do, I think.
		return
	}

	// figure out which fields stay, and where they are in each of
	// the tuple types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := fieldMap(e1, e2)

	// assign fields from the old relation to fields in the new
	body1 := make(chan T)

	body2 := make(chan T)
	go func(body chan T) {
		for tup1 := range body {
			tup2 := reflect.Indirect(reflect.New(e2))
			rtup1 := reflect.ValueOf(tup1)
			for _, fm := range fMap {
				tupf2 := tup2.Field(fm.j)
				tupf2.Set(rtup1.Field(fm.i))
			}
			// set the field in the new tuple to the value
			// from the old one
			body2 <- tup2.Interface()
		}
		close(body2)
	}(r.body)

	// figure out which of the candidate keys (if any) to keep.
	// only the keys that only have attributes in the new type are
	// valid.  If we do have any keys that are still valid, then
	// we don't have to perform distinct on the body.

	if len(cn2) == 0 {
		// make a new primary key and ensure the results are distinct
		r.cKeys = append(r.cKeys, cn2)
		r.body = distinct(body2)
	} else {
		r.body = body2
	}

}

// Zero returns the zero value of the relation (a blank tuple)
func (r *ProjectExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *ProjectExpr) CKeys() CandKeys {
	z1 := r.source.Zero()

	cKeys := r.source.CandKeys()

	// first figure out if the tuple types of the relation and projection are
	// equivalent.  If so, we don't have to do anything with the candidate
	// keys.
	e1 := reflect.TypeOf(z1)
	e2 := reflect.TypeOf(r.zero)

	if e1.AssignableTo(e2) {
		// nothing to do, I think.
		return cKeys
	}

	// otherwise we have to subset the candidate keys.
	fMap := fieldMap(e1, e2)

	h1 := r.source.Heading()
	cn1 := make([]string, len(h1))
	for i, att := range h1 {
		cn1[i] = att.Name
	}
	cKeys = subsetCandidateKeys(cKeys, cn1, fMap)

	// every relation except dee and dum have at least one candidate key
	if len(cKeys) == 0 {
		cKeys = defaultKeys(r.zero)
	}

	return cKeys
}

// text representation

const projectSymbol = "π"

// GoString returns a text representation of the Relation
func (r *ProjectExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *ProjectExpr) String() string {
	return stringTabTable(r)
}

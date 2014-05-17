// restriction implements a restrict expression in relational algebra

package rel

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
type RestrictExpr struct {
	// the input relation
	source Relation

	// the restriction predicate
	p reflect.Value

	// the subdomain of the function input
	subd reflect.Value
}

/*
func (r *Simple) RestrictExpr(p Predicate) {
	// take the internal channel and apply a predicate to it

	// channel of the output tuples
	body2 := make(chan T)

	// transform the body so that it only sends values that pass the
	// predicate
	go func(body chan T) {
		for tup := range body {
			if p(tup) {
				body2 <- tup
			}
		}
		close(body2)
	}()

	r.body = body2
}
*/
func (r *RestrictExpr) Tuples(t chan T) {
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
func (r *RestrictExpr) Zero() T {
	return r.source.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *RestrictExpr) CKeys() CandKeys {
	return r.source.CandKeys()
}

// text representation
const restrictSymbol = "ρ"

// GoString returns a text representation of the Relation
func (r *RestrictExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *RestrictExpr) String() string {
	return stringTabTable(r)
}

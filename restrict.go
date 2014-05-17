// restrict implements a restrict expression in relational algebra

package rel

import "reflect"

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
type RestrictExpr struct {
	// the input relation
	source Relation

	// the restriction predicate
	p Predicate

	// the subdomain of the function input
	subd reflect.Type
}

func (r RestrictExpr) Tuples(t chan T) {
	// transform the channel of tuples from the relation
	z1 := r.source.Zero()

	e1 := reflect.TypeOf(z1)
	e2 := r.subd

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := fieldMap(e1, e2)

	// TODO(jonlawlor) add parallelism here
	body1 := make(chan T)
	r.source.Tuples(body1)
	go func(body chan T, res chan T, p Predicate) {
		parm := make([]reflect.Value, 1)
		for tup1 := range body {
			tup2 := reflect.Indirect(reflect.New(e2))
			rtup1 := reflect.ValueOf(tup1)
			for _, fm := range fMap {
				tupf2 := tup2.Field(fm.j)
				tupf2.Set(rtup1.Field(fm.i))
			}

			// call the predicate with the new tuple to determine if it should
			// go into the results
			parm[0] = tup2
			if b := reflect.ValueOf(p).Call(parm); b[0].Interface().(bool) {
				res <- tup1
			}
		}
		close(res)
	}(body1, t, r.p)

	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r RestrictExpr) Zero() T {
	return r.source.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r RestrictExpr) CKeys() CandKeys {
	return r.source.CKeys()
}

// text representation
const restrictSymbol = "ρ"

// GoString returns a text representation of the Relation
func (r RestrictExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r RestrictExpr) String() string {
	return stringTabTable(r)
}

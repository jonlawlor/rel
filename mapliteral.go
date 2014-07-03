// mapLiteral is a relation with underlying data stored in a map.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a map with key as structs (but empty struct values!!!).
//
// One nice thing is that Maps will never have duplicate keys, which makes them
// a natural way to represent relations.

package rel

import (
	"reflect"
)

// mapLiteral is an implementation of Relation using a map
type mapLiteral struct {

	// the map of tuples in the relation, with tuples in the key
	rbody reflect.Value // should always hold a map

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero interface{}

	// first error encountered during construction or during evaluation
	err error
}

// TupleChan sends each tuple in the relation to a channel
func (r *mapLiteral) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	go func(res reflect.Value) {
		// output channel
		resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}
		canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}

		for _, tup := range r.rbody.MapKeys() {
			resSel.Send = tup
			chosen, _, _ := reflect.Select([]reflect.SelectCase{canSel, resSel})
			if chosen == 0 {
				// cancel has been closed, so close the results
				return
			}
		}
		chv.Close()
	}(chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *mapLiteral) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *mapLiteral) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *mapLiteral) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *mapLiteral) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *mapLiteral) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *mapLiteral) Restrict(p Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
func (r1 *mapLiteral) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *mapLiteral) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *mapLiteral) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *mapLiteral) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *mapLiteral) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *mapLiteral) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *mapLiteral) Err() error {
	return r1.err
}

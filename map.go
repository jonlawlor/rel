// Map is a relation with underlying data stored in a map.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a map with key as structs (but no values!!!).
//
// One nice thing is that Maps will never have duplicate keys.

package rel

// Map is an implementation of Relation using a map
type Map struct {

	// the channel of tuples in the relation
	body map[T]struct{}

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T
}

// Tuples sends each tuple in the relation to a channel
func (r *Map) Tuples(t chan T) {
	go func() {
		for tup, _ := range r.body {
			t <- tup
		}
		close(t)
	}()
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
	return stringTabTable(r)
}

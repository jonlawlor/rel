// Chan is a relation with underlying data stored in a channel.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a chan of structs.
//

package rel

// Chan is an implementation of Relation using a channel
type Chan struct {
	// heading constitute the heading of the relation.
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to
	// use the methods defined on a particular struct.

	// the channel of tuples in the relation
	body chan T

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
// TODO(jonlawlor) change the rel.New constructor so that it doesn't consume
// one of the input channel's values, and instead include it here.
func (r *Chan) Tuples(t chan T) {
	go func() {
		for tup := range r.body {
			t <- tup
		}
		close(t)
	}()
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *Chan) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *Chan) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *Chan) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *Chan) String() string {
	return stringTabTable(r)
}

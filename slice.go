// slice implements one of the possible ways of creating a new relation from
// scratch, specifically, with a slice of structs

package rel

// slice represents a relation that came from a slice of a struct
type Slice struct {
	// the slice of tuples in the relation
	body []T

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *Slice) Tuples(t chan T) {
	go func() {
		for v := range r.body {
			t <- v
		}
		close(t)
	}()
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *Slice) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *Slice) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *Slice) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *Slice) String() string {
	return stringTabTable(r)
}

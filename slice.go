// slice implements one of the possible ways of creating a new relation from
// scratch, specifically, with a slice of structs

package rel

import "reflect"

// slice represents a relation that came from a slice of a struct
type Slice struct {
	// the slice of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T

	// sourceDistinct indicates if the source slice was already distinct or if
	// a distinct has to be performed when sending tuples
	sourceDistinct bool
}

// Tuples sends each tuple in the relation to a channel
// and when it is finished it closes the input channel.
func (r *Slice) Tuples(t chan T) {
	if r.sourceDistinct {
		go func(rbody reflect.Value, res chan T) {
			for i := 0; i < rbody.Len(); i++ {
				res <- rbody.Index(i).Interface()
			}
			close(res)
		}(r.rbody, t)
		return
	}

	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[T]struct{}{}
	go func(rbody reflect.Value, res chan T) {
		for i := 0; i < rbody.Len(); i++ {
			tup := rbody.Index(i).Interface()
			if _, dup := mem[tup]; !dup {
				res <- tup
				mem[tup] = struct{}{}
			}
		}
		close(res)
	}(r.rbody, t)
	return
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

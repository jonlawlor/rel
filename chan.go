// Chan is a relation with underlying data stored in a channel.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a chan of structs.
//

package rel

import "reflect"

// Chan is an implementation of Relation using a channel
type Chan struct {
	// the channel of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T

	// sourceDistinct indicates if the source chan was already distinct or if a
	// distinct has to be performed when sending tuples
	sourceDistinct bool
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *Chan) Tuples(t chan T) {
	if r.sourceDistinct {
		go func(rbody reflect.Value, res chan T) {
			for {
				rtup, ok := rbody.Recv()
				if !ok {
					break
				}
				res <- rtup.Interface()
			}
			close(res)
		}(r.rbody, t)
		return
	}
	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[T]struct{}{}
	go func(rbody reflect.Value, res chan T) {
		for {
			rtup, ok := rbody.Recv()
			if !ok {
				break
			}
			tup := rtup.Interface()
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

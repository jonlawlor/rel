// chanLiteral is a relation with underlying data stored in a channel.
// It is intended to be used for general purpose source data that can only be
// queried as a whole, or that has been preprocessed.  It can also be used as
// an adapter to interface with other sources of data.  Basically anything
// that can produce a chan of structs.
//

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// chanLiteral is an implementation of Relation using a channel
type chanLiteral struct {
	// the channel of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys att.CandKeys

	// the type of the tuples contained within the relation
	zero interface{}

	// sourceDistinct indicates if the source chan was already distinct or if a
	// distinct has to be performed when sending tuples
	sourceDistinct bool

	err error
}

// TupleChan sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *chanLiteral) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := att.EnsureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}
	if r.sourceDistinct {
		go func(rbody, res reflect.Value) {
			// input channel
			sourceSel := reflect.SelectCase{reflect.SelectRecv, rbody, reflect.Value{}}
			canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
			inCases := []reflect.SelectCase{canSel, sourceSel}

			// output channel
			resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}

			for {
				chosen, tup, ok := reflect.Select(inCases)

				// cancel has been closed, so close the results
				if chosen == 0 {
					return
				}
				if !ok {
					// source channel was closed
					break
				}
				resSel.Send = tup
				outCases := []reflect.SelectCase{canSel, resSel}
				chosen, _, _ = reflect.Select(outCases)

				if chosen == 0 {
					// cancel has been closed, so close the results
					return
				}
			}
			res.Close()
		}(r.rbody, chv)
		return cancel
	}
	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[interface{}]struct{}{}
	go func(rbody, res reflect.Value) {
		sourceSel := reflect.SelectCase{reflect.SelectRecv, rbody, reflect.Value{}}
		canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
		inCases := []reflect.SelectCase{canSel, sourceSel}

		resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}

		for {
			chosen, rtup, ok := reflect.Select(inCases)
			// cancel has been closed, so close the results
			if chosen == 0 {
				return
			}
			if !ok {
				// source channel was closed
				break
			}

			tup := interface{}(rtup.Interface())
			if _, dup := mem[tup]; !dup {
				resSel.Send = rtup
				chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					// cancel has been closed, so close the results
					return
				}
				mem[tup] = struct{}{}
			}
		}
		res.Close()
	}(r.rbody, chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *chanLiteral) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *chanLiteral) CKeys() att.CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *chanLiteral) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *chanLiteral) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *chanLiteral) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *chanLiteral) Restrict(p att.Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *chanLiteral) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *chanLiteral) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
//
func (r1 *chanLiteral) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *chanLiteral) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *chanLiteral) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *chanLiteral) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *chanLiteral) Err() error {
	return r1.err
}

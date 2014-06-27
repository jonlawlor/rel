//setdiff implements a set difference expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// setDiffExpr implements a set difference in relational algebra
// This is one of the operations which consumes memory.  In addition, no values
// can be sent before all values from the second source are consumed.
type setDiffExpr struct {
	source1 Relation
	source2 Relation

	err error
}

func (r *setDiffExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := att.EnsureChan(chv.Type(), r.Zero())
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	mem := make(map[interface{}]struct{})
	// setdiff is unique in that it has to immediately consume all of the
	// values from the second relation in order to send any values in the
	// first one.  All other relational operations can be done lazily, this
	// one can only be done half-lazy.
	// with some indexing this is avoidable, though.
	// In addition, we could start pulling values from the first relation as
	// well if we were willing to later go through them.  However, this will
	// always use more memory, so we can leave it up to the caller.
	// Alternatively, we could pull one value from the first relation, and then
	// discard it if we recieve a match from the second.  Then we would have to
	// go back through previously recieved values after receiving the from the
	// first relation again.  That would require a mutex on mem.

	// tuples in both sides should have the same type, checked during
	// construction
	e := reflect.TypeOf(r.source1.Zero())

	// create channels over the body of the source relations
	body1 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel1 := r.source1.TupleChan(body1.Interface())
	body2 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel2 := r.source2.TupleChan(body2.Interface())

	go func(b1, b2, res reflect.Value) {
		// first pull all of the values from the second relation, because
		// we need them all before we can produce a single value from the
		// first
		// input channels
		source1Sel := reflect.SelectCase{reflect.SelectRecv, b1, reflect.Value{}}
		source2Sel := reflect.SelectCase{reflect.SelectRecv, b2, reflect.Value{}}
		canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
		inCases := []reflect.SelectCase{canSel, source2Sel}

		// output channels
		resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}

		for {
			chosen, tup, ok := reflect.Select(inCases)
			if chosen == 0 || !ok {
				// cancel channel was closed
				break
			}
			mem[tup.Interface()] = struct{}{}
		}

		inCases[1] = source1Sel
		for {
			chosen, tup, ok := reflect.Select(inCases)
			if chosen == 0 || !ok {
				// cancel channel was closed
				break
			}
			if _, rem := mem[tup.Interface()]; !rem {
				resSel.Send = tup
				chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					break
				}
			}
		}
		select {
		case <-cancel:
			close(bcancel1)
			close(bcancel2)
		default:
			if err := r.source1.Err(); err != nil {
				r.err = err
			} else if err := r.source2.Err(); err != nil {
				r.err = err
			}
			res.Close()
		}
	}(body1, body2, chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *setDiffExpr) Zero() interface{} {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *setDiffExpr) CKeys() att.CandKeys {
	return r.source1.CKeys()
}

// GoString returns a text representation of the Relation
func (r *setDiffExpr) GoString() string {
	return r.source1.GoString() + ".SetDiff(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *setDiffExpr) String() string {
	return r.source1.String() + " − " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *setDiffExpr) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *setDiffExpr) Restrict(p att.Predicate) Relation {
	return NewSetDiff(r1.source1.Restrict(p), r1.source2.Restrict(p))
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *setDiffExpr) Rename(z2 interface{}) Relation {
	return NewSetDiff(r1.source1.Rename(z2), r1.source2.Rename(z2))
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *setDiffExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *setDiffExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *setDiffExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *setDiffExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *setDiffExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *setDiffExpr) Err() error {
	return r1.err
}

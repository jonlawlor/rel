//diff implements a set difference expression in relational algebra

package rel

import (
	"reflect"
)

// diffExpr implements a set difference in relational algebra
// This is one of the operations which consumes memory.  In addition, no values
// can be sent before all values from the second source are consumed.
type diffExpr struct {
	source1 Relation
	source2 Relation
	err     error
}

// TupleChan sends each tuple in the relation to a channel
func (r *diffExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r.Zero())
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	mem := make(map[interface{}]struct{})
	// TODO(jonlawlor): we could pull one value from the first relation, and then
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
		source1Sel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: b1}
		source2Sel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: b2}
		canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
		inCases := []reflect.SelectCase{canSel, source2Sel}

		// output channels
		resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}

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
func (r *diffExpr) Zero() interface{} {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *diffExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// GoString returns a text representation of the Relation
func (r *diffExpr) GoString() string {
	return r.source1.GoString() + ".Diff(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *diffExpr) String() string {
	return r.source1.String() + " − " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *diffExpr) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// Restrict can be distributed through diff.
func (r1 *diffExpr) Restrict(p Predicate) Relation {
	return NewDiff(r1.source1.Restrict(p), r1.source2.Restrict(p))
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// Rename can be distributed through setdiff
func (r1 *diffExpr) Rename(z2 interface{}) Relation {
	return NewDiff(r1.source1.Rename(z2), r1.source2.Rename(z2))
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *diffExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *diffExpr) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *diffExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *diffExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *diffExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *diffExpr) Err() error {
	return r1.err
}

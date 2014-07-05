// map implements a set mapping expression in relational algebra.  It has the
// same name as a builtin type so we might want to rename it?

package rel

import (
	"reflect"
)

// map is a type that represents applying a function to each tuple in a source
// set.
type mapExpr struct {
	// the input relation
	source1 Relation

	// zero is the resulting relation tuple type
	zero interface{}

	// valTYpe is the tuple type of the values provided to the mapping
	// function.
	valType reflect.Type

	// resType is the tuple type of the values returned from the mapping
	// function.
	resType reflect.Type

	// the function that maps from source tuple type to result tuple type
	rmfcn reflect.Value

	// set of candidate keys
	cKeys CandKeys

	// sourceDistinct indicates if the function results in distinct tuples or
	// if a distinct has to be performed afterwards
	isDistinct bool

	// err contains the first error encountered during construction or evaluation
	err error
}

// TupleChan sends each tuple in the relation to a channel
func (r1 *mapExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r1.zero)
	if err != nil {
		r1.err = err
		return cancel
	}
	if r1.err != nil {
		chv.Close()
		return cancel
	}

	// figure out the new elements used for each of the derived types
	e1 := reflect.TypeOf(r1.source1.Zero())

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := FieldMap(e1, r1.valType)

	body1 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e1), 0)
	bcancel := r1.source1.TupleChan(body1.Interface())

	if r1.isDistinct {
		// assign fields from the old relation to fields in the new
		// TODO(jonlawlor): add parallelism here
		go func(body, res reflect.Value) {
			// input channels
			sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
			canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
			inCases := []reflect.SelectCase{canSel, sourceSel}

			// output channels
			resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}

			for {
				chosen, tup, ok := reflect.Select(inCases)

				if chosen == 0 {
					// cancel has been closed, so close the source as well
					close(bcancel)
					return
				}
				if !ok {
					// source channel was closed
					break
				}

				// construct the function input
				fcnin := reflect.Indirect(reflect.New(r1.valType))
				for _, fm := range fMap {
					fcninf := fcnin.Field(fm.J)
					fcninf.Set(tup.Field(fm.I))
				}
				// set the field in the new tuple to the value from the old one

				fcnout := r1.rmfcn.Call([]reflect.Value{fcnin})[0]
				resSel.Send = fcnout
				chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					close(bcancel)
					return
				}
			}
			if err := r1.source1.Err(); err != nil {
				r1.err = err
			}
			res.Close()
		}(body1, chv)

		return cancel
	}
	go func(body, res reflect.Value) {
		m := map[interface{}]struct{}{}

		// input channels
		sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
		canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
		inCases := []reflect.SelectCase{canSel, sourceSel}

		// output channels
		resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}

		for {
			chosen, tup, ok := reflect.Select(inCases)

			if chosen == 0 {
				// cancel has been closed, so close the source as well
				close(bcancel)
				return
			}
			if !ok {
				// source channel was closed
				break
			}

			// construct the function input
			fcnin := reflect.Indirect(reflect.New(r1.valType))
			for _, fm := range fMap {
				fcninf := fcnin.Field(fm.J)
				fcninf.Set(tup.Field(fm.I))
			}
			// set the field in the new tuple to the value from the old one

			fcnout := r1.rmfcn.Call([]reflect.Value{fcnin})[0]

			// check that the output from the function is not a duplicate
			if _, isdup := m[fcnout.Interface()]; !isdup {
				// it isn't a dupe, so send it on the results
				m[fcnout.Interface()] = struct{}{}
				resSel.Send = fcnout
				chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					close(bcancel)
					return
				}
			}
		}

		if err := r1.source1.Err(); err != nil {
			r1.err = err
		}
		chv.Close()
	}(body1, chv)

	return cancel

}

// Zero returns the zero value of the relation (a blank tuple)
func (r1 *mapExpr) Zero() interface{} {
	return r1.zero
}

// CKeys is the set of candidate keys in the relation
func (r1 *mapExpr) CKeys() CandKeys {
	return r1.cKeys
}

// GoString returns a text representation of the Relation
func (r1 *mapExpr) GoString() string {
	return goStringTabTable(r1)
}

// String returns a text representation of the Relation
func (r1 *mapExpr) String() string {
	return r1.source1.String() + ".Map({" + HeadingString(r1.source1) + "}->{" + HeadingString(r1) + "})"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *mapExpr) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *mapExpr) Restrict(p Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
func (r1 *mapExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *mapExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *mapExpr) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *mapExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *mapExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *mapExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *mapExpr) Err() error {
	return r1.err
}

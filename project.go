// project implements a project expression in relational algebra

package rel

import (
	"reflect"
)

// projection is a type that represents a project operation
type projectExpr struct {
	// the input relation
	source1 Relation

	// the new tuple type
	zero interface{}

	// first error encountered during construction or evaluation
	err error
}

// TupleChan sends each tuple in the relation to a channel
func (r1 *projectExpr) TupleChan(t interface{}) chan<- struct{} {
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

	// transform the channel of tuples from the relation
	z1 := r1.source1.Zero()
	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e1 := reflect.TypeOf(z1)
	e2 := reflect.TypeOf(r1.zero)

	// create the channel of tuples from source
	// TODO(jonlawlor): restrict the channel direction
	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e1), 0)
	bcancel := r1.source1.TupleChan(body.Interface())

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	fMap := FieldMap(e1, e2)

	// figure out if we need to distinct the results because there are no
	// candidate keys left
	// TODO(jonlawlor): refactor with the code in the CKeys() method, or
	// include in an isDistinct field?
	cKeys := SubsetCandidateKeys(r1.source1.CKeys(), Heading(r1.source1), fMap)
	if len(cKeys) == 0 {
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
				// cancel has been closed, so close the results
				if chosen == 0 {
					close(bcancel)
					return
				}
				if !ok {
					// source channel was closed
					break
				}
				tup2 := reflect.Indirect(reflect.New(e2))
				for _, fm := range fMap {
					tupf2 := tup2.Field(fm.J)
					tupf2.Set(tup.Field(fm.I))
				}
				// set the field in the new tuple to the value from the old one
				if _, isdup := m[tup2.Interface()]; !isdup {
					m[tup2.Interface()] = struct{}{}
					resSel.Send = tup2
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
			res.Close()
		}(body, chv)
		return cancel
	}

	// assign fields from the old relation to fields in the new
	// TODO(jonlawlor) add parallelism here
	go func(body, res reflect.Value) {

		// input channels
		sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
		canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
		inCases := []reflect.SelectCase{canSel, sourceSel}

		// output channels
		resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}

		for {
			chosen, tup, ok := reflect.Select(inCases)
			// cancel has been closed, so close the results
			if chosen == 0 {
				close(bcancel)
				return
			}
			if !ok {
				// source channel was closed
				break
			}
			tup2 := reflect.Indirect(reflect.New(e2))
			for _, fm := range fMap {
				tupf2 := tup2.Field(fm.J)
				tupf2.Set(tup.Field(fm.I))
			}

			resSel.Send = tup2
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
	}(body, chv)

	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r1 *projectExpr) Zero() interface{} {
	return r1.zero
}

// CKeys is the set of candidate keys in the relation
func (r1 *projectExpr) CKeys() CandKeys {
	z1 := r1.source1.Zero()

	cKeys := r1.source1.CKeys()

	// first figure out if the tuple types of the relation and projection are
	// equivalent.  If so, we don't have to do anything with the candidate
	// keys.
	e1 := reflect.TypeOf(z1)
	e2 := reflect.TypeOf(r1.zero)

	if e1.AssignableTo(e2) {
		// nothing to do
		return cKeys
	}

	// otherwise we have to subset the candidate keys.
	fMap := FieldMap(e1, e2)
	cKeys = SubsetCandidateKeys(cKeys, Heading(r1.source1), fMap)

	// every relation except dee and dum have at least one candidate key
	if len(cKeys) == 0 {
		cKeys = DefaultKeys(r1.zero)
	}

	return cKeys
}

// text representation

// GoString returns a text representation of the Relation
func (r1 *projectExpr) GoString() string {
	return r1.source1.GoString() + ".Project(" + HeadingString(r1) + ")"
}

// String returns a text representation of the Relation
func (r1 *projectExpr) String() string {
	return "π{" + HeadingString(r1) + "}(" + r1.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
// This can always be rewritten as a project of the source, and skip the
// intermediate project.
func (r1 *projectExpr) Project(z2 interface{}) Relation {
	return NewProject(r1.source1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This can always be rewritten to pass the restrict up the relational
// expression.
func (r1 *projectExpr) Restrict(p Predicate) Relation {
	return NewProject(r1.source1.Restrict(p), r1.zero)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
func (r1 *projectExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *projectExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *projectExpr) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *projectExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *projectExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *projectExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *projectExpr) Err() error {
	return r1.err
}

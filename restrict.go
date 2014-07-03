// restrict implements a restrict expression in relational algebra

package rel

import (
	"reflect"
	"runtime"
	"sync"
)

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an subdomain of the tuples of the
// relation, and returns a boolean.
type restrictExpr struct {
	// the input relation
	source1 Relation

	// the restriction predicate
	p Predicate

	// err is the first error encountered during construction or evaluation
	err error
}

// TupleChan sends each tuple in the relation to a channel
func (r *restrictExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r.source1.Zero())
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	// transform the channel of tuples from the relation
	// TODO(jonlawlor): add a mechanism for concurrency to be modified.
	mc := runtime.GOMAXPROCS(-1)

	z1 := r.source1.Zero()
	e1 := reflect.TypeOf(z1)

	predFunc := reflect.ValueOf(r.p.EvalFunc(e1))

	// create the channel of tuples from source
	// TODO(jonlawlor): restrict the channel direction
	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e1), 0)
	bcancel := r.source1.TupleChan(body.Interface())

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res reflect.Value) {
		wg.Wait()
		// if we've been cancelled, send it up to the source
		select {
		case <-cancel:
			close(bcancel)
		default:
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			res.Close()
		}
	}(chv)

	for i := 0; i < mc; i++ {
		go func(body, res reflect.Value, p Predicate) {
			// input channels
			sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
			canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
			inCases := []reflect.SelectCase{canSel, sourceSel}

			// output channels
			resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}
			for {
				chosen, tup, ok := reflect.Select(inCases)

				if chosen == 0 || !ok {
					// source channel was closed, or we ran out of source
					break
				}

				// call the predicate with the new tuple to determine if it should
				// go into the results
				tf := predFunc.Call([]reflect.Value{tup})

				if tf[0].Bool() {
					resSel.Send = tup
					chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
					if chosen == 0 {
						break
					}
				}
			}
			wg.Done()
		}(body, chv, r.p)
	}
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *restrictExpr) Zero() interface{} {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *restrictExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// GoString returns a text representation of the Relation
func (r *restrictExpr) GoString() string {
	return r.source1.GoString() + ".Restrict(" + r.p.String() + ")"
}

// String returns a text representation of the Relation
func (r *restrictExpr) String() string {
	return "σ{" + r.p.String() + "}(" + r.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
// Project can be rewritten if the Predicate can be evaluated on the Project's
// results.
func (r1 *restrictExpr) Project(z2 interface{}) Relation {
	att2 := FieldNames(reflect.TypeOf(z2))
	if IsSubDomain(r1.p.Domain(), att2) { // the predicate's attributes exist after project
		return NewRestrict(r1.source1.Project(z2), r1.p)
	} else {
		return NewProject(r1, z2)
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// Restrict can be rewritten by switching the order of inputs, which may allow
// some predicates to pass through to source relations.
func (r1 *restrictExpr) Restrict(p Predicate) Relation {
	// try reversing the order, which may allow some lower degree restrictions
	// to pass through
	return NewRestrict(r1.source1.Restrict(p), r1.p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
func (r1 *restrictExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *restrictExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *restrictExpr) Diff(r2 Relation) Relation {
	// TODO(jonlawlor): we could apply the predicate to r2 and produce the
	// same result?
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *restrictExpr) Join(r2 Relation, zero interface{}) Relation {
	// TODO(jonlawlor): we could sometimes apply the predicate to r2 and reduce
	// the number of comparisons in the join.
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *restrictExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *restrictExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *restrictExpr) Err() error {
	return r1.err
}

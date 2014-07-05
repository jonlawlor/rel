// union implements a union expression in relational algebra

package rel

import (
	"reflect"
	"runtime"
	"sync"
)

// unionExpr represents a union expression in relational algebra.
// This is one of the relational operations which consumes memory.
type unionExpr struct {
	// source relations for the union
	source1 Relation
	source2 Relation

	// err has the last error encountered during construction or evaluation.
	err error
}

// TupleChan sends each tuple in the relation to a channel
func (r1 *unionExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r1.Zero())
	if err != nil {
		r1.err = err
		return cancel
	}
	if r1.err != nil {
		chv.Close()
		return cancel
	}

	// TODO(jonlawlor): allow the caller to have more control over the
	// amount of concurrency?
	mc := runtime.GOMAXPROCS(-1)

	var mu sync.Mutex
	mem := make(map[interface{}]struct{})

	// tuples in both sides should have the same type, checked during
	// construction
	e := reflect.TypeOf(r1.source1.Zero())

	// create channels over the body of the source relations
	body1 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel1 := r1.source1.TupleChan(body1.Interface())
	body2 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel2 := r1.source2.TupleChan(body2.Interface())

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res reflect.Value) {
		wg.Wait()
		select {
		case <-cancel:
			close(bcancel1)
			close(bcancel2)
		default:
			if err := r1.source1.Err(); err != nil {
				r1.err = err
			} else if err := r1.source2.Err(); err != nil {
				r1.err = err
			}
			res.Close()
		}
	}(chv)

	for i := 0; i < mc; i++ {
		go func(b1, b2, res reflect.Value) {
			// input channels
			source1Sel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: b1}
			source2Sel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: b2}
			canSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cancel)}
			neverRecv := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(make(chan struct{}))}
			inCases := []reflect.SelectCase{canSel, source1Sel, source2Sel}

			// output channels
			resSel := reflect.SelectCase{Dir: reflect.SelectSend, Chan: res}

			openSources := 2
			for openSources > 0 {
				chosen, tup, ok := reflect.Select(inCases)
				if chosen == 0 {
					// cancel channel was closed
					break
				}
				if chosen > 0 && !ok {
					// one of the bodies completed
					inCases[chosen] = neverRecv
					openSources--
					continue
				}

				// otherwise we've received a new value from one of the sources
				mu.Lock()
				if _, dup := mem[tup.Interface()]; !dup {
					mem[tup.Interface()] = struct{}{}
					mu.Unlock()
					resSel.Send = tup
					chosen, _, ok = reflect.Select([]reflect.SelectCase{canSel, resSel})
					if chosen == 0 {
						break
					}
				} else {
					mu.Unlock()
				}
			}
			wg.Done()
		}(body1, body2, chv)
	}
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r1 *unionExpr) Zero() interface{} {
	return r1.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r1 *unionExpr) CKeys() CandKeys {
	// unions have the intersection of the source candidate keys
	// the keys are sorted on length and then alphabetically, which helps
	// reduce the number of comparisons needed.
	cKeys1 := r1.source1.CKeys()
	cKeys2 := r1.source2.CKeys()

	var cKeysRes [][]Attribute

	// this can only happen if either relation is dee or dum
	if len(cKeys1) == 0 || len(cKeys2) == 0 {
		return cKeysRes
	}

	j := 0
Loop1:
	for _, ck1 := range cKeys1 {
		if len(ck1) < len(cKeys2[j]) {
			continue
		}
		for len(ck1) > len(cKeys2[j]) {
			if len(cKeys2) == j {
				return cKeysRes
			}
			j++
		}
	Loop2:
		for len(ck1) == len(cKeys2[j]) {
			// compare each of the attributes in the candidate keys
			for k := range ck1 {
				if ck1[k] < cKeys2[j][k] {
					continue Loop1
				} else if ck1[k] > cKeys2[j][k] {
					j++
					if j == len(cKeys2) {
						return cKeysRes
					}
					continue Loop2
				}
			}
			cKeysRes = append(cKeysRes, ck1)
			// We should only get a single match for a given candidate key, so
			// we can advance j as well.
			j++
			if j == len(cKeys2) {
				return cKeysRes
			}
			break Loop2
		}
	}
	return cKeysRes
}

// GoString returns a text representation of the Relation
func (r1 *unionExpr) GoString() string {
	return r1.source1.GoString() + ".Union(" + r1.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r1 *unionExpr) String() string {
	return r1.source1.String() + " ∪ " + r1.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
// Project can distribute over a union.
func (r1 *unionExpr) Project(z2 interface{}) Relation {
	return NewUnion(r1.source1.Project(z2), r1.source2.Project(z2))
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup interface{}) bool where tup is a subdomain of the input r.
// Restrict can distribute over a union.
func (r1 *unionExpr) Restrict(p Predicate) Relation {
	return NewUnion(r1.source1.Restrict(p), r1.source2.Restrict(p))
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// rename can distribute over a union.
func (r1 *unionExpr) Rename(z2 interface{}) Relation {
	return NewUnion(r1.source1.Rename(z2), r1.source2.Rename(z2))
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *unionExpr) Union(r2 Relation) Relation {
	// It might be useful to define a multiple union?  There would be a memory
	// benefit in some cases.
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *unionExpr) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *unionExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *unionExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *unionExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *unionExpr) Err() error {
	return r1.err
}

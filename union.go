// union implements a union expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
	"runtime"
	"sync"
)

// unionExpr represents a union expression in relational algebra.
// This is one of the relational operations which consumes memory.
type unionExpr struct {
	source1 Relation
	source2 Relation

	err error
}

func (r *unionExpr) TupleChan(t interface{}) chan<- struct{} {
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

	mc := runtime.GOMAXPROCS(-1)

	var mu sync.Mutex
	mem := make(map[interface{}]struct{})

	// tuples in both sides should have the same type, checked during
	// construction
	e := reflect.TypeOf(r.source1.Zero())

	// create channels over the body of the source relations
	body1 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel1 := r.source1.TupleChan(body1.Interface())
	body2 := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	bcancel2 := r.source2.TupleChan(body2.Interface())

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res reflect.Value) {
		wg.Wait()
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
	}(chv)

	for i := 0; i < mc; i++ {
		go func(b1, b2, res reflect.Value) {
			// input channels
			source1Sel := reflect.SelectCase{reflect.SelectRecv, b1, reflect.Value{}}
			source2Sel := reflect.SelectCase{reflect.SelectRecv, b2, reflect.Value{}}
			canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
			neverRecv := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(make(chan struct{})), reflect.Value{}}
			inCases := []reflect.SelectCase{canSel, source1Sel, source2Sel}

			// output channels
			resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}

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
func (r *unionExpr) Zero() interface{} {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *unionExpr) CKeys() att.CandKeys {
	// unions have the intersection of the source candidate keys
	// the keys are sorted on length and then alphabetically, which helps
	// reduce the number of comparisons needed.
	cKeys1 := r.source1.CKeys()
	cKeys2 := r.source2.CKeys()

	cKeysRes := make([][]att.Attribute, 0)

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
					} else {
						continue Loop2
					}
				}
			}
			cKeysRes = append(cKeysRes, ck1)
			// We should only get a single match for a given candidate key, so
			// we can advance j as well.
			j++
			if j == len(cKeys2) {
				return cKeysRes
			} else {
				break Loop2
			}
		}
	}
	return cKeysRes
}

// GoString returns a text representation of the Relation
func (r *unionExpr) GoString() string {
	return r.source1.GoString() + ".Union(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *unionExpr) String() string {
	return r.source1.String() + " ∪ " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *unionExpr) Project(z2 interface{}) Relation {
	return NewUnion(r1.source1.Project(z2), r1.source2.Project(z2))
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup interface{}) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *unionExpr) Restrict(p att.Predicate) Relation {
	return NewUnion(r1.source1.Restrict(p), r1.source2.Restrict(p))
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *unionExpr) Rename(z2 interface{}) Relation {
	return NewUnion(r1.source1.Rename(z2), r1.source2.Rename(z2))
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *unionExpr) Union(r2 Relation) Relation {
	// It might be useful to define a multiple union?  There would be a memory
	// benefit in some cases.
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
//
func (r1 *unionExpr) Diff(r2 Relation) Relation {
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *unionExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *unionExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *unionExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *unionExpr) Err() error {
	return r1.err
}

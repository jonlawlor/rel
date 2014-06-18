// union implements a union expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
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

func (r *unionExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	mc := runtime.GOMAXPROCS(-1)

	var mu sync.Mutex
	mem := make(map[interface{}]struct{})

	body1 := make(chan interface{})
	body2 := make(chan interface{})
	bcancel1 := r.source1.Tuples(body1)
	bcancel2 := r.source2.Tuples(body2)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan<- interface{}) {
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
			close(res)
		}
	}(t)

	for i := 0; i < mc; i++ {
		go func(b1, b2 <-chan interface{}, res chan<- interface{}) {
		Loop:
			for b1 != nil || b2 != nil {
				select {
				case tup1, ok := <-b1:
					if !ok {
						b1 = nil
						break
					}
					mu.Lock()
					if _, dup := mem[tup1]; !dup {
						mem[tup1] = struct{}{}
						mu.Unlock()
						select {
						case res <- tup1:
						case <-cancel:
							break Loop
						}
					} else {
						mu.Unlock()
					}
				case tup2, ok := <-b2:
					if !ok {
						b2 = nil
						break
					}
					mu.Lock()
					if _, dup := mem[tup2]; !dup {
						mem[tup2] = struct{}{}
						mu.Unlock()
						select {
						case res <- tup2:
						case <-cancel:
							break Loop
						}
					} else {
						mu.Unlock()
					}
				case <-cancel:
					break Loop
				}
			}
			wg.Done()
		}(body1, body2, t)
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

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *unionExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *unionExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *unionExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *unionExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *unionExpr) Err() error {
	return r1.err
}

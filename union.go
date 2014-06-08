// union implements a union expression in relational algebra

package rel

import (
	"reflect"
	"runtime"
	"sync"
)

// UnionExpr represents a union expression in relational algebra.
// This is one of the relational operations which consumes memory.
type UnionExpr struct {
	source1 Relation
	source2 Relation
}

func (r *UnionExpr) Tuples(t chan<- T) chan<- struct{} {
	cancel := make(chan struct{})
	mc := runtime.GOMAXPROCS(-1)

	var mu sync.Mutex
	mem := make(map[interface{}]struct{})

	body1 := make(chan T)
	body2 := make(chan T)
	bcancel1 := r.source1.Tuples(body1)
	bcancel2 := r.source2.Tuples(body2)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan<- T) {
		wg.Wait()
		select {
		case <-cancel:
			close(bcancel1)
			close(bcancel2)
		default:
			close(res)
		}
	}(t)

	for i := 0; i < mc; i++ {
		go func(b1, b2 <-chan T, res chan<- T) {
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
func (r *UnionExpr) Zero() T {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r *UnionExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// GoString returns a text representation of the Relation
func (r *UnionExpr) GoString() string {
	return r.source1.GoString() + ".Union(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *UnionExpr) String() string {
	return r.source1.String() + " ∪ " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *UnionExpr) Project(z2 T) Relation {
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &UnionExpr{r1.source1.Project(z2), r1.source2.Project(z2)}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *UnionExpr) Restrict(p Predicate) Relation {
	return &UnionExpr{r1.source1.Restrict(p), r1.source2.Restrict(p)}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *UnionExpr) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *UnionExpr) Union(r2 Relation) Relation {
	// It might be useful to define a multiple union?  There would be a memory
	// benefit in some cases.
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *UnionExpr) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *UnionExpr) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *UnionExpr) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

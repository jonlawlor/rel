// union implements a union expression in relational algebra

package rel

import (
	"runtime"
	"sync"
)

// UnionExpr represents a union expression in relational algebra.
// This is one of the relational operations which consumes memory.
type UnionExpr struct {
	source1 Relation
	source2 Relation
}

func (r UnionExpr) Tuples(t chan T) {

	mc := runtime.GOMAXPROCS(-1)

	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan T) {
		wg.Wait()
		close(res)
	}(t)

	var mu sync.Mutex
	mem := make(map[interface{}]struct{})

	body1 := make(chan T)
	body2 := make(chan T)
	go r.source1.Tuples(body1)
	go r.source2.Tuples(body2)

	for i := 0; i < mc; i++ {
		go func(b1, b2, res chan T) {
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
						res <- tup1
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
						res <- tup2
					} else {
						mu.Unlock()
					}
				}
			}
			wg.Done()
		}(body1, body2, t)
	}
	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r UnionExpr) Zero() T {
	return r.source1.Zero()
}

// CKeys is the set of candidate keys in the relation
func (r UnionExpr) CKeys() CandKeys {
	return r.source1.CKeys()
}

// text representation
const unionSymbol = "∪"

// GoString returns a text representation of the Relation
func (r UnionExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r UnionExpr) String() string {
	return stringTabTable(r)
}

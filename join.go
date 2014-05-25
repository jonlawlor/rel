// join implements a natural join expression in relational algebra

package rel

import (
	"reflect"
	"runtime"
	"sync"
)

type JoinExpr struct {
	source1 Relation
	source2 Relation
	zero    T
}

func (r JoinExpr) Tuples(t chan T) {
	mc := runtime.GOMAXPROCS(-1)
	e3 := reflect.TypeOf(r.zero)

	// create indexes between the three headings
	h1 := Heading(r.source1)
	h2 := Heading(r.source2)
	h3 := Heading(r)

	map12 := attributeMap(h1, h2) // used to determine equality
	map31 := attributeMap(h3, h1) // used to construct returned values
	map32 := attributeMap(h3, h2) // used to construct returned values

	// create channels over the body of the source relations
	body1 := make(chan T)
	body2 := make(chan T)
	r.source1.Tuples(body1)
	r.source2.Tuples(body2)

	// Create the memory of previously sent tuples so that the joins can
	// continue to compare against old values.
	var mu sync.Mutex
	mem1 := make([]reflect.Value, 0)
	mem2 := make([]reflect.Value, 0)

	// wg is used to signal when each of the worker goroutines finishes
	// processing the join operation
	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan T) {
		wg.Wait()
		close(res)
	}(t)

	// create a go routine that generates the join for each of the input tuples
	// TODO(jonlawlor): In retrospect, there is absolutely no way this will
	// work without some memory.  There has to be a better way to do this.  As
	// it is, if the first relation is not a map or slice, it will not produce
	// correct results.  Even if it is, the result will only be found after a
	// lot of needless computation.
	for i := 0; i < mc; i++ {
		go func(b1, b2, res chan T) {
			for {
				select {
				case tup1, ok := <-b1:
					if !ok {
						b1 = nil
						break
					}
					// lock both memories, first to add b1 onto mem1 and then
					// to make a copy of mem2
					rtup1 := reflect.ValueOf(tup1)
					mu.Lock()
					mem1 = append(mem1, rtup1)
					m2tups := mem2[:]
					mu.Unlock()
					for _, rtup2 := range m2tups {
						if partialEquals(rtup1, rtup2, map12) {
							tup3 := reflect.Indirect(reflect.New(e3))
							combineTuples2(&tup3, rtup1, map31)
							combineTuples2(&tup3, rtup2, map32)
							res <- tup3.Interface()
						}
					}

				case tup2, ok := <-b2:
					if !ok {
						b2 = nil
						break
					}
					rtup2 := reflect.ValueOf(tup2)
					mu.Lock()
					mem2 = append(mem2, rtup2)
					m1tups := mem1[:]
					mu.Unlock()
					for _, rtup1 := range m1tups {
						if partialEquals(rtup1, rtup2, map12) {
							tup3 := reflect.Indirect(reflect.New(e3))
							combineTuples2(&tup3, rtup1, map31)
							combineTuples2(&tup3, rtup2, map32)
							res <- tup3.Interface()

						}
					}
				}
				if b1 == nil && b2 == nil {
					wg.Done()
					break
				}
			}
		}(body1, body2, t)
	}

	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r JoinExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r JoinExpr) CKeys() CandKeys {
	// TODO(jonlawlor): determine new candidate keys.  This is just a
	// placeholder
	return r.source1.CKeys()
}

// text representation
const joinSymbol = "⋈"

// GoString returns a text representation of the Relation
func (r JoinExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r JoinExpr) String() string {
	return stringTabTable(r)
}

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

func (r *JoinExpr) Tuples(t chan<- T) {
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
	go func(res chan<- T) {
		wg.Wait()
		close(res)
	}(t)

	// create a go routine that generates the join for each of the input tuples
	for i := 0; i < mc; i++ {
		go func(b1, b2 <-chan T, res chan<- T) {
			for b1 != nil || b2 != nil {
				select {
				case tup1, ok := <-b1:
					if !ok {
						b1 = nil
						break
					}
					// lock both memories, first to add b1 onto mem1 and then
					// to make a copy of mem2
					// TODO(jonlawlor): refactor this code along with the other
					// case.
					rtup1 := reflect.ValueOf(tup1)
					mu.Lock()
					mem1 = append(mem1, rtup1)
					m2tups := mem2[:]
					mu.Unlock()

					// Send tuples that match previously retrieved tuples in
					// the opposite relation.  This is nice because it operates
					// concurrently.
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
			}
			wg.Done()
		}(body1, body2, t)
	}

	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *JoinExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *JoinExpr) CKeys() CandKeys {
	// TODO(jonlawlor): determine new candidate keys.  This is just a
	// placeholder
	return r.source1.CKeys()
}

// text representation
const joinSymbol = "⋈"

// GoString returns a text representation of the Relation
func (r *JoinExpr) GoString() string {
	return r.source1.GoString() + ".Join(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *JoinExpr) String() string {
	return r.source1.String() + " ⋈ " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *JoinExpr) Project(z2 T) Relation {
	// TODO(jonlawlor): this can be sped up if we compare the candidate keys
	// used in the relation to the new domain, along with the source relations
	// domains.
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &ProjectExpr{r1, z2}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *JoinExpr) Restrict(p Predicate) Relation {

	// decompose compound predicates
	if andPred, ok := p.(AndPred); ok {
		// this covers some theta joins
		return r1.Restrict(andPred.P1).Restrict(andPred.P2)
	}

	dom := p.Domain()
	h1 := Heading(r1.source1)
	h2 := Heading(r1.source2)
	if isSubDomain(dom, h1) {
		if isSubDomain(dom, h2) {
			return &JoinExpr{r1.source1.Restrict(p), r1.source2.Restrict(p), r1.zero}
		} else {
			return &JoinExpr{r1.source1.Restrict(p), r1.source2, r1.zero}
		}
	} else if isSubDomain(dom, h2) {
		return &JoinExpr{r1.source1, r1.source2.Restrict(p), r1.zero}
	} else {
		return &RestrictExpr{r1, p}
	}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *JoinExpr) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *JoinExpr) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *JoinExpr) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *JoinExpr) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *JoinExpr) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

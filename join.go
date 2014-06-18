// join implements a natural join expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
	"runtime"
	"sync"
)

type joinExpr struct {
	source1 Relation
	source2 Relation
	zero    interface{}

	err error
}

func (r *joinExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	mc := runtime.GOMAXPROCS(-1)
	e3 := reflect.TypeOf(r.zero)

	// create indexes between the three headings
	h1 := Heading(r.source1)
	h2 := Heading(r.source2)
	h3 := Heading(r)

	map12 := att.AttributeMap(h1, h2) // used to determine equality
	map31 := att.AttributeMap(h3, h1) // used to construct returned values
	map32 := att.AttributeMap(h3, h2) // used to construct returned values

	// create channels over the body of the source relations
	body1 := make(chan interface{})
	body2 := make(chan interface{})
	bcancel1 := r.source1.Tuples(body1)
	bcancel2 := r.source2.Tuples(body2)

	// Create the memory of previously sent tuples so that the joins can
	// continue to compare against old values.
	var mu sync.Mutex
	mem1 := make([]reflect.Value, 0)
	mem2 := make([]reflect.Value, 0)

	// wg is used to signal when each of the worker goroutines finishes
	// processing the join operation
	var wg sync.WaitGroup
	wg.Add(mc)
	go func(res chan<- interface{}) {
		wg.Wait()
		// if we've been cancelled, send it up to the source
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

	// create a go routine that generates the join for each of the input tuples
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
						if att.PartialEquals(rtup1, rtup2, map12) {
							tup3 := reflect.Indirect(reflect.New(e3))
							att.CombineTuples2(&tup3, rtup1, map31)
							att.CombineTuples2(&tup3, rtup2, map32)
							select {
							case res <- tup3.Interface():
							case <-cancel:
								break Loop
							}
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
						if att.PartialEquals(rtup1, rtup2, map12) {
							tup3 := reflect.Indirect(reflect.New(e3))
							att.CombineTuples2(&tup3, rtup1, map31)
							att.CombineTuples2(&tup3, rtup2, map32)
							select {
							case res <- tup3.Interface():
							case <-cancel:
								break Loop
							}
						}
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
func (r *joinExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *joinExpr) CKeys() att.CandKeys {
	// the candidate keys of a join are a join of the candidate keys as well
	cKeys1 := r.source1.CKeys()
	cKeys2 := r.source2.CKeys()

	cKeysRes := make([][]att.Attribute, 0)

	for _, ck1 := range cKeys1 {
		for _, ck2 := range cKeys2 {
			ck := make([]att.Attribute, len(ck1))
			copy(ck, ck1)
		Loop:
			for j := range ck2 {
				for i := range ck {
					if ck2[j] == ck[i] {
						continue Loop
					}
				}
				ck = append(ck, ck2[j])
			}
			cKeysRes = append(cKeysRes, ck)
		}
	}
	att.OrderCandidateKeys(cKeysRes)
	return cKeysRes
}

// GoString returns a text representation of the Relation
func (r *joinExpr) GoString() string {
	return r.source1.GoString() + ".Join(" + r.source2.GoString() + ")"
}

// String returns a text representation of the Relation
func (r *joinExpr) String() string {
	return r.source1.String() + " ⋈ " + r.source2.String()
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *joinExpr) Project(z2 interface{}) Relation {
	// TODO(jonlawlor): this can be sped up if we compare the candidate keys
	// used in the relation to the new domain, along with the source relations
	// domains.
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *joinExpr) Restrict(p att.Predicate) Relation {
	// decompose compound predicates
	if andPred, ok := p.(att.AndPred); ok {
		// this covers some theta joins
		return r1.Restrict(andPred.P1).Restrict(andPred.P2)
	}

	dom := p.Domain()
	h1 := Heading(r1.source1)
	h2 := Heading(r1.source2)
	if att.IsSubDomain(dom, h1) {
		if att.IsSubDomain(dom, h2) {
			return r1.source1.Restrict(p).Join(r1.source2.Restrict(p), r1.zero)
		} else {
			return r1.source1.Restrict(p).Join(r1.source2, r1.zero)
		}
	} else if att.IsSubDomain(dom, h2) {
		return r1.source1.Join(r1.source2.Restrict(p), r1.zero)
	} else {
		return NewRestrict(r1, p)
	}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *joinExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *joinExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *joinExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *joinExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *joinExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *joinExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *joinExpr) Err() error {
	return r1.err
}

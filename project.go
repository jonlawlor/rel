// project implements a project expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// projection is a type that represents a project operation
type projectExpr struct {
	// the input relation
	source1 Relation

	// the new tuple type
	zero interface{}

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *projectExpr) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	// transform the channel of tuples from the relation
	z1 := r.source1.Zero()
	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e1 := reflect.TypeOf(z1)
	e2 := reflect.TypeOf(r.zero)

	body1 := make(chan interface{})
	bcancel := r.source1.Tuples(body1)

	if e1.AssignableTo(e2) {
		// nothing to do.
		go func(body <-chan interface{}, res chan<- interface{}) {
		Loop:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop
					}
					select {
					case res <- tup1:
					case <-cancel:
						close(bcancel)
						return
					}

				case <-cancel:
					close(bcancel)
					return
				}
			}
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			close(res)
		}(body1, t)
		return cancel
	}

	// figure out which fields stay, and where they are in each of the tuple
	// types.
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.
	fMap := att.FieldMap(e1, e2)

	// figure out if we need to distinct the results because there are no
	// candidate keys left
	// TODO(jonlawlor): refactor with the code in the CKeys() method
	cKeys := att.SubsetCandidateKeys(r.source1.CKeys(), Heading(r.source1), fMap)
	if len(cKeys) == 0 {
		go func(body <-chan interface{}, res chan<- interface{}) {
			m := map[interface{}]struct{}{}
		Loop:
			for {
				select {
				case tup1, ok := <-body:
					if !ok {
						break Loop
					}
					tup2 := reflect.Indirect(reflect.New(e2))
					rtup1 := reflect.ValueOf(tup1)
					for _, fm := range fMap {
						tupf2 := tup2.Field(fm.J)
						tupf2.Set(rtup1.Field(fm.I))
					}
					// set the field in the new tuple to the value from the old one
					if _, isdup := m[tup2.Interface()]; !isdup {
						m[tup2.Interface()] = struct{}{}
						select {
						case res <- tup2.Interface():
						case <-cancel:
							close(bcancel)
							return
						}
					}
				case <-cancel:
					close(bcancel)
					return
				}
			}
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			close(res)
		}(body1, t)
		return cancel
	}

	// assign fields from the old relation to fields in the new
	// TODO(jonlawlor) add parallelism here
	go func(body <-chan interface{}, res chan<- interface{}) {
	Loop:
		for {
			select {
			case tup1, ok := <-body:
				if !ok {
					break Loop
				}
				tup2 := reflect.Indirect(reflect.New(e2))
				rtup1 := reflect.ValueOf(tup1)
				for _, fm := range fMap {
					tupf2 := tup2.Field(fm.J)
					tupf2.Set(rtup1.Field(fm.I))
				}
				select {
				// set the field in the new tuple to the value from the old one
				case res <- tup2.Interface():
				case <-cancel:
					close(bcancel)
					return
				}
			case <-cancel:
				close(bcancel)
				return
			}
		}
		if err := r.source1.Err(); err != nil {
			r.err = err
		}
		close(res)
	}(body1, t)

	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *projectExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *projectExpr) CKeys() att.CandKeys {
	z1 := r.source1.Zero()

	cKeys := r.source1.CKeys()

	// first figure out if the tuple types of the relation and projection are
	// equivalent.  If so, we don't have to do anything with the candidate
	// keys.
	e1 := reflect.TypeOf(z1)
	e2 := reflect.TypeOf(r.zero)

	if e1.AssignableTo(e2) {
		// nothing to do
		return cKeys
	}

	// otherwise we have to subset the candidate keys.
	fMap := att.FieldMap(e1, e2)
	cKeys = att.SubsetCandidateKeys(cKeys, Heading(r.source1), fMap)

	// every relation except dee and dum have at least one candidate key
	if len(cKeys) == 0 {
		cKeys = att.DefaultKeys(r.zero)
	}

	return cKeys
}

// text representation

// GoString returns a text representation of the Relation
func (r *projectExpr) GoString() string {
	return r.source1.GoString() + ".Project(" + HeadingString(r) + ")"
}

// String returns a text representation of the Relation
func (r *projectExpr) String() string {
	return "π{" + HeadingString(r) + "}(" + r.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *projectExpr) Project(z2 interface{}) Relation {
	return NewProject(r1.source1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *projectExpr) Restrict(p att.Predicate) Relation {
	return NewProject(r1.source1.Restrict(p), r1.zero)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *projectExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *projectExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *projectExpr) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *projectExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *projectExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	return NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *projectExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *projectExpr) Err() error {
	return r1.err
}

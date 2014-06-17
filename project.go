// project implements a project expression in relational algebra

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// projection is a type that represents a project operation
type ProjectExpr struct {
	// the input relation
	source1 Relation

	// the new tuple type
	zero interface{}

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *ProjectExpr) Tuples(t chan<- interface{}) chan<- struct{} {
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
func (r *ProjectExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *ProjectExpr) CKeys() att.CandKeys {
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
func (r *ProjectExpr) GoString() string {
	return r.source1.GoString() + ".Project(" + HeadingString(r) + ")"
}

// String returns a text representation of the Relation
func (r *ProjectExpr) String() string {
	return "π{" + HeadingString(r) + "}(" + r.source1.String() + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *ProjectExpr) Project(z2 interface{}) Relation {
	if r1.Err() != nil {
		return r1
	}
	// the second project will always override the first
	return &ProjectExpr{r1.source1, z2, nil}

}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *ProjectExpr) Restrict(p att.Predicate) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &ProjectExpr{r1.source1.Restrict(p), r1.zero, nil}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *ProjectExpr) Rename(z2 interface{}) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RenameExpr{r1, z2, nil}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *ProjectExpr) Union(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &UnionExpr{r1, r2, nil}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *ProjectExpr) SetDiff(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &SetDiffExpr{r1, r2, nil}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *ProjectExpr) Join(r2 Relation, zero interface{}) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &JoinExpr{r1, r2, zero, nil}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *ProjectExpr) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &GroupByExpr{r1, t2, vt, gfcn, nil}
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *ProjectExpr) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	if r1.Err() != nil {
		return r1
	}
	// determine the type of the returned tuples
	r := new(MapExpr)
	r.source1 = r1
	r.zero = z2
	r.fcn = mfcn
	if len(ckeystr) == 0 {
		// all relations have a candidate key of all of their attributes, or
		// a non zero subset if the relation is not dee or dum
		r.cKeys = att.DefaultKeys(z2)
	} else {
		r.isDistinct = true
		// convert from [][]string to CandKeys
		r.cKeys = att.String2CandKeys(ckeystr)
	}
	return r
}

// Error returns an error encountered during construction or computation
func (r1 *ProjectExpr) Err() error {
	return r1.err
}

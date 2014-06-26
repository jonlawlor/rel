// sliceLiteral implements one of the possible ways of creating a new relation from
// scratch, specifically, with a slice of structs

package rel

import (
	"github.com/jonlawlor/rel/att"
	"reflect"
)

// sliceLiteral represents a relation that came from a slice of a struct
type sliceLiteral struct {
	// the slice of tuples in the relation
	rbody reflect.Value

	// set of candidate keys
	cKeys att.CandKeys

	// the type of the tuples contained within the relation
	zero interface{}

	// sourceDistinct indicates if the source slice was already distinct or if
	// a distinct has to be performed when sending tuples
	sourceDistinct bool

	err error
}

func (r *sliceLiteral) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := ensureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}
	if r.sourceDistinct {
		go func(rbody, res reflect.Value) {

			// output channels
			canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
			resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}
			for i := 0; i < rbody.Len(); i++ {
				resSel.Send = rbody.Index(i)
				chosen, _, _ := reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					return
				}
			}
			res.Close()
		}(r.rbody, chv)
		return cancel
	}

	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	go func(rbody, res reflect.Value) {
		mem := map[interface{}]struct{}{}

		// output channels
		resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}
		canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}

		for i := 0; i < rbody.Len(); i++ {
			rtup := rbody.Index(i)

			if _, dup := mem[rtup.Interface()]; !dup {
				mem[rtup.Interface()] = struct{}{}

				resSel.Send = rtup
				chosen, _, _ := reflect.Select([]reflect.SelectCase{canSel, resSel})
				if chosen == 0 {
					return
				}
			}
		}
		res.Close()
	}(r.rbody, chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *sliceLiteral) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *sliceLiteral) CKeys() att.CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *sliceLiteral) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *sliceLiteral) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *sliceLiteral) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *sliceLiteral) Restrict(p att.Predicate) Relation {
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *sliceLiteral) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *sliceLiteral) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *sliceLiteral) SetDiff(r2 Relation) Relation {
	return NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *sliceLiteral) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *sliceLiteral) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *sliceLiteral) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *sliceLiteral) Err() error {
	return r1.err
}

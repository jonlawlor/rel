// the Simple type is a relation with underlying data stored in a struct
// slice.  It is intended to be a starter implementation that can be used
// to check the validity of more complicated approaches.
//
// It makes heavy use of reflection, but should provide some interesting
// ways of programming in go.  Because it uses so much reflection, it is
// difficult to implement in an idiomatic way.  Also, the performance
// leaves something to be desired!  However, once the interface is complete
// it might be possible to implement it in more efficient ways.
//

package rel

import (
	"reflect"
)

// Simple is an implementation of Relation using a []struct
type Simple struct {
	// Names & Types constitute the heading of the relation.
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to
	// use the methods defined on a particular struct.
	Names []string
	Types []reflect.Type

	// I wish there was a more precise way of representing this?
	Body []reflect.Value

	// set of candidate keys
	CKeys [][]string

	// the type of the tuples contained within the relation
	tupleType reflect.Type
}

// Deg returns the degree of the relation
func (r Simple) Deg() int {
	return len(r.Names)
}

// Card returns the cardinality of the relation
func (r Simple) Card() int {
	return len(r.Body)
}

// Tuples sends each tuple in the relation to a channel
func (r Simple) Tuples(t chan reflect.Value) {
	go func() {
		defer close(t)
		for _, tup := range r.Body {
			t <- tup
		}
	}()
	return
}

// Heading returns a map from column names to types
func (r Simple) Heading() []Attribute {
	deg := r.Deg()
	h := make([]Attribute, deg)
	for i := 0; i < deg; i++ {
		h[i] = Attribute{r.Names[i], r.Types[i]}
	}
	return h
}

// String returns a text representation of the Relation
func (r Simple) GoString() string {
	return goStringTabTable(r)
}

func (r Simple) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subset of the current tuple's
// type.  We can't use a slice of strings because go can't construct
// arbitrary types through reflection.
func (r1 Simple) Project(t2 interface{}) (r2 Relation) {
	c := r1.Card()
	ck1 := r1.CKeys
	b2 := make([]reflect.Value, c)

	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e2 := reflect.TypeOf(t2)
	if r1.tupleType.AssignableTo(e2) {
		for i, tup := range r1.Body {
			b2[i] = tup
		}
		return Simple{r1.Names, r1.Types, b2, ck1, e2}
	}

	// figure out which fields stay, and where they are in each of
	// the tuple types.
	fMap := fieldMap(r1.tupleType, e2)
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.

	// assign fields from the old relation to fields in the new
	for i, tup1 := range r1.Body {
		tup2 := reflect.Indirect(reflect.New(e2))
		for _, fm := range fMap {
			tupf2 := tup2.Field(fm.j)
			tupf2.Set(tup1.Field(fm.i))
		}
		// set the field in the new tuple to the value
		// from the old one
		b2[i] = tup2
	}

	// figure out which of the candidate keys (if any) to keep.
	// only the keys that only have attributes in the new type are
	// valid.  If we do have any keys that are still valid, then
	// we don't have to perform distinct on the body again.

	// figure out the names to remove from the original data
	remNames := make(map[string]struct{})
	for _, n1 := range r1.Names {
		if _, keyfound := fMap[n1]; !keyfound {
			remNames[n1] = struct{}{}
		}
	}

	ck2 := make([][]string, 0)
KeyLoop:
	for _, ck := range ck1 {
		// if the candidate key contains a name we want to remove, then
		// get rid of it
		for _, k := range ck {
			if _, keyfound := remNames[k]; keyfound {
				continue KeyLoop
			}
		}
		ck2 = append(ck2, ck)
	}

	cn, ct := namesAndTypes(e2)
	if len(ck2) == 0 {
		// create a new primary key
		// I'm not sure this implementation has good
		// performance.
		m := make(map[interface{}]struct{})
		for _, tup2 := range b2 {
			m[tup2.Interface()] = struct{}{}
		}
		b2 = make([]reflect.Value, len(m))
		i := 0
		for tup2 := range m {
			b2[i] = reflect.ValueOf(tup2)
			i++
		}
		ck2 = append(ck2, []string{})
		copy(ck2[0], cn)
	}
	// construct the returned relation
	return Simple{cn, ct, b2, ck2, e2}
}

// union is a set union of two relations
func (r1 Simple) Union(r2 Relation) Relation {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	// turn the first relation into a map and then add on the values from
	// the second one, then return the keys as a new relation
	m := make(map[reflect.Value]struct{}, r1.Card()+r2.Card())
	for _, tup1 := range r1.Body {
		m[tup1] = struct{}{}
	}

	// the second relation has to return its values through a channel
	tups := make(chan reflect.Value)
	r2.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?  Also, it might be possible to make 2 maps instead of a single
	// map, and populate them concurrently, and at the end merge them.
	// It may be more efficient to store the relation bodies in maps if we
	// always have to construct a map to do anything with them.
	for tup2 := range tups {
		m[tup2] = struct{}{}
	}
	b := make([]reflect.Value, len(m))
	i := 0
	for tup, _ := range m {
		b[i] = tup
		i++
	}
	// return the new relation
	// TODO(jonlawlor): should these be copies?
	return Simple{r1.Names, r1.Types, b, r1.CKeys, r1.tupleType}
}

// setdiff returns the set difference of the two relations
func (r1 Simple) SetDiff(r2 Relation) (onlyr1 Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.
	m := make(map[reflect.Value]struct{}, r1.Card())
	for _, tup1 := range r1.Body {
		m[tup1] = struct{}{}
	}

	// the second relation has to return its values through a channel
	tups := make(chan reflect.Value)
	r2.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	for tup2 := range tups {
		delete(m, tup2)
	}
	b := make([]reflect.Value, len(m))
	i := 0
	for tup, _ := range m {
		b[i] = tup
		i++
	}

	// return the new relation
	// TODO(jonlawlor): should these be copies?
	onlyr1 = Simple{r1.Names, r1.Types, b, r1.CKeys, r1.tupleType}
	return
}

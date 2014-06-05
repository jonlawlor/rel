// Package rel implements relational algebra.
// The relational algebra here follows in the footsteps of "Database in Depth"
// by C. J. Date.  Therefore all terminology should be the same as used in that
// book.  There are some notable differences from SQL.
package rel

// variable naming conventions
//
// r, r1, r2, r3, ... all represent relations.  If there is an operation which
// has an output relation, the output relation will have the highest number
// after the r.
//
// body, body1, body2, b, b1, b2, ... all represent channels of tuples.
//
// zero, z, z1, z2, ... all represent a tuple's zero value, with defaults in
// all of the fields.
//
// e, e1, e2, ... all represent the reflect.ValueOf(z) with the appropriate
// identification.
//
// tup, tup1, tup2, ... all represent actual tuples going through some
// relational transformation.
//
// rtup, rtup1, rtup2, ... all represent the reflect.ValueOf(tup) with the
// appropriate identification.

import (
	"fmt" // we might want to replace this with the errors package?
	"reflect"
	"strings"
)

// T is represents tuples, and it should always be a struct
type T interface{}

// Attribute represents a particular attribute's name in a relation
type Attribute string

// CandKeys is a set of candidate keys
// they should be unique and sorted
type CandKeys [][]Attribute

// Relation has similar meaning to tables in SQL
type Relation interface {
	// Zero is the zero value for the tuple
	Zero() T

	// CKeys is the set of candidate keys for the Relation
	CKeys() CandKeys

	// Tuples takes a channel of interface and keeps sending
	// the tuples in the relation over the channel.
	// should this be allowed to consume an internal channel?
	Tuples(chan T) // does this channel need a direction?

	// the following methods are a part of relational algebra

	Project(z2 T) Relation

	Restrict(p Predicate) Relation

	Rename(z2 T) Relation

	Union(r2 Relation) Relation

	SetDiff(r2 Relation) Relation

	Join(r2 Relation, zero T) Relation

	// non relational but still useful

	GroupBy(t2, vt T, gfcn func(chan T) T) Relation

	// not necessary but still very useful

	String() string

	GoString() string
}

// New creates a new Relation from a []struct, map[struct] or chan struct.
func New(v interface{}, ckeystr [][]string) Relation {

	// depending on the type of the input, we represent a relation in different
	// types of relation.
	rbody := reflect.ValueOf(v)
	e := reflect.TypeOf(v).Elem()
	z := reflect.Indirect(reflect.New(e)).Interface()

	switch rbody.Kind() {
	case reflect.Map:
		r := new(Map)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			// maps are already distinct on the key, so the Map relation type
			// does not have a sourceDistinct field.  Maps are probably the
			// most natural way of storing relations.

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = defaultKeys(z)
		} else {
			// convert from [][]string to CandKeys
			r.cKeys = string2CandKeys(ckeystr)
		}
		r.zero = z
		// we might want to check the candidate keys for validity here?
		orderCandidateKeys(r.cKeys)
		return r

	case reflect.Chan:
		r := new(Chan)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			r.cKeys = defaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = string2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z
		// we might want to check the candidate keys for validity here?
		orderCandidateKeys(r.cKeys)
		return r

	case reflect.Slice:
		r := new(Slice)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			r.cKeys = defaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = string2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z

		// we might want to check the candidate keys for validity here?
		orderCandidateKeys(r.cKeys)
		return r
	default:
		panic(fmt.Sprintf("unrecognized relation kind: %v", rbody.Kind()))
	}
}

// Heading is a slice of column names
func Heading(r Relation) []Attribute {
	return fieldNames(reflect.TypeOf(r.Zero()))
}

// HeadingString is a string representation of the attributes of a relation
// formatted like "{foo, bar}"
func HeadingString(r Relation) string {
	h := Heading(r)
	s := make([]string, len(h))
	for i, v := range h {
		s[i] = string(v)
	}
	return strings.Join(s, ", ")
}

func GoString(r Relation) string {
	return goStringTabTable(r)
}

// Deg returns the degree of the relation
func Deg(r Relation) int {
	return len(Heading(r))
}

// Card returns the cardinality of the relation
// note: this consumes the values of the relation's tuples and can be an
// expensive operation.  We might want per-relation implementation of this?
// Alternatively we can use a different interface to determine if the caller
// also implements its own Card someplace else, and just leave this
// implementation as default.
func Card(r Relation) (i int) {
	body := make(chan T)
	r.Tuples(body)
	for _ = range body {
		i++
	}
	return
}

// additional derived functions
// SemiDiff(r2 Relation) Relation
// SemiJoin(r2 Relation) Relation

// probably want to add non-Relational functions like
// Update
// Insert
// some kind of ordering?

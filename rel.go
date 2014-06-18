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
	"github.com/jonlawlor/rel/att"
	"reflect"
	"strings"
)

// Relation has similar meaning to tables in SQL
type Relation interface {
	// Zero is the zero value for the tuple
	Zero() interface{}

	// CKeys is the set of candidate keys for the Relation
	CKeys() att.CandKeys

	// Tuples takes a channel of interface and keeps sending
	// the tuples in the relation over the channel.
	// It returns a "cancel" channel that can be used to halt computations
	// early
	Tuples(chan<- interface{}) (cancel chan<- struct{})

	// the following methods are a part of relational algebra

	Project(z2 interface{}) Relation

	Restrict(p att.Predicate) Relation

	Rename(z2 interface{}) Relation

	Union(r2 Relation) Relation

	SetDiff(r2 Relation) Relation

	Join(r2 Relation, zero interface{}) Relation

	Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation

	// non relational but still useful

	GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation

	// not necessary but still very useful!

	String() string

	GoString() string

	Err() error
}

// New creates a new Relation from a []struct, map[struct] or chan struct.
// TODO(jonlawlor): decide if NewSlice, NewMap, NewChan would be more
//appropriate.
func New(v interface{}, ckeystr [][]string) Relation {

	// depending on the type of the input, we represent a relation in different
	// types of relation.
	rbody := reflect.ValueOf(v)
	e := reflect.TypeOf(v).Elem()
	z := reflect.Indirect(reflect.New(e)).Interface()

	switch rbody.Kind() {
	case reflect.Map:
		r := new(mapLiteral)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			// maps are already distinct on the key, so the Map relation type
			// does not have a sourceDistinct field.  Maps are probably the
			// most natural way of storing relations.

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = att.DefaultKeys(z)
		} else {
			// convert from [][]string to CandKeys
			r.cKeys = att.String2CandKeys(ckeystr)
		}
		r.zero = z
		// we might want to check the candidate keys for validity here?
		att.OrderCandidateKeys(r.cKeys)
		return r

	case reflect.Chan:
		r := new(chanLiteral)
		r.rbody = rbody // TODO(jonlawlor): check direction
		if len(ckeystr) == 0 {
			r.cKeys = att.DefaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = att.String2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z
		// we might want to check the candidate keys for validity here?
		att.OrderCandidateKeys(r.cKeys)
		return r

	case reflect.Slice:
		r := new(sliceLiteral)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			r.cKeys = att.DefaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = att.String2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z

		// we might want to check the candidate keys for validity here?
		att.OrderCandidateKeys(r.cKeys)
		return r
	default:
		panic(fmt.Sprintf("unrecognized relation kind: %v", rbody.Kind()))
	}
}

// Heading is a slice of column names
func Heading(r Relation) []att.Attribute {
	return att.FieldNames(reflect.TypeOf(r.Zero()))
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
	body := make(chan interface{})
	_ = r.Tuples(body)
	for _ = range body {
		i++
	}
	return
}

// NewProject creates a new relation expression with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func NewProject(r1 Relation, z2 interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	att2 := att.FieldNames(reflect.TypeOf(z2))
	//TODO(jonlawlor): test that z2 is a subset of r1's zero
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &projectExpr{r1, z2, nil}
	}
}

// NewRestrict creates a new relation expression with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func NewRestrict(r1 Relation, p att.Predicate) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	return &restrictExpr{r1, p, nil}
}

// NewRename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func NewRename(r1 Relation, z2 interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	return &renameExpr{r1, z2, nil}
}

// NewUnion creates a new relation by unioning the bodies of both inputs
//
func NewUnion(r1, r2 Relation) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	return &unionExpr{r1, r2, nil}
}

// NewSetDiff creates a new relation by set minusing the two inputs
//
func NewSetDiff(r1, r2 Relation) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	return &setDiffExpr{r1, r2, nil}
}

// NewJoin creates a new relation by performing a natural join on the inputs
//
func NewJoin(r1, r2 Relation, zero interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	return &joinExpr{r1, r2, zero, nil}
}

// NewGroupBy creates a new relation by grouping and applying a user defined func
//
func NewGroupBy(r1 Relation, t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	return &groupByExpr{r1, t2, vt, gfcn, nil}
}

// NewMap creates a new relation by applying a function to tuples in the source
func NewMap(r1 Relation, mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	// determine the type of the returned tuples
	r := new(mapExpr)
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

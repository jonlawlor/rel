// rel is a package that implements relational algebra
// the relational algebra here follows in the footsteps of "Database in
// Depth" by C. J. Date.  Therefore all terminology should be the same as
// used in that book.  There are some notable differences from SQL - the
// biggest of which is that all Relations are automatically distinct.
// The second biggest is that there are no nulls.  If you need a type to
// represent a null, you'll have to add it in yourself.
// Also, all relations have at least one candidate key, there are two
// relations with no attributes, and there is no primary key in the base
// interface.
//
// The current implementation:
// It makes heavy use of reflection, but should provide some interesting
// ways of programming in go.  Because it uses so much reflection, it is
// difficult to implement in an idiomatic way.  Also, the performance
// leaves something to be desired!  However, once the interface is complete
// it might be possible to implement it in more efficient ways.
//
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

	// these are not relational but they are sure nice to have
	GoString() string
	String() string
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

// The following methods generate relation expressions, also called queries.
// The resulting type xxxExpr will typically implement some additional
// interfaces that are used to infer when reordering is possible, such as
// DistributeProjecter, which would indicate that the project operation is
// distributable over the xxxExpr operation.
// http://www.dcs.warwick.ac.uk/~wmb/CS319/pdf/opt.pdf gives a quick summary
// of some of the relational algebra laws.
// In this way, client types, such as an SQLTable relation, can implement
// a DistributeProjecter interface, which would then allow us to limit the
// number of attributes fetched on the database side.
// Another question is how to represent non relational operations, such as
// groupby, which has an implicit project.

// There is also a question of how much work to do during the initial setup of
// a new relation expression, when we might end up reordering it later.

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func Project(r1 Relation, z2 T) ProjectExpr {
	return ProjectExpr{r1, z2}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func Restrict(r Relation, p Predicate) RestrictExpr {
	return RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func Rename(r1 Relation, z2 T) RenameExpr {
	return RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func Union(r1, r2 Relation) UnionExpr {
	return UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func SetDiff(r1, r2 Relation) SetDiffExpr {
	return SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func Join(r1, r2 Relation, zero T) JoinExpr {
	return JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func GroupBy(r1 Relation, t2, vt T, gfcn func(chan T) T) GroupByExpr {
	return GroupByExpr{r1, t2, vt, gfcn}
}

// additional derived functions
// SemiDiff(r2 Relation) Relation
// SemiJoin(r2 Relation) Relation

// probably want to add non-Relational functions like
// Update
// Insert
// some kind of ordering?

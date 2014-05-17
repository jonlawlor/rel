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

import (
	"reflect"
)

// T is used to represent tuples
type T interface{}

// Attribute represents a Name:Type pair which defines the heading
// of the relation
// I'm not sure this should be exported.
type Attribute struct {
	Name string
	Type reflect.Type
}

// CandKeys is a set of candidate keys
// they should be unique and sorted
type CandKeys [][]string

// Predicate is the type of func that takes a tuple and returns bool
// and is used for restrict.  It should always be a func with input of a
// subdomain of the relation, with one bool output.
type Predicate interface{}

// theta is the type of func used as a predicate in theta-joins
// it should have type func(tup1 T, tup2 T) bool
// where tup1 is a subdomain of the left relation and tup2 is a subdomain of
// the right relation.
type Theta interface{}

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

// New creates a new Relation.
func New(v interface{}, ckeystr [][]string) Relation {

	// depending on the type of the input, we represent a relation in different
	// ways.
	rval := reflect.ValueOf(v)
	e := rval.Elem()
	z := e.Interface()

	switch rval.Kind() {
	case reflect.Map:
		r := new(Map)
		r.zero = z
		r.cKeys = CandKeys(ckeystr)

		r.body = make(map[interface{}]struct{}, rval.Len())
		mkeys := rval.MapKeys()
		for v := range mkeys {
			r.body[v.Interface()] = struct{}{}
		}

		if len(r.cKeys) == 0 {
			// maps are already distinct on the key

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = defaultKeys(r.zero)

			// change the body to use a distinct channel instead of an assumed
			// distinct channel
		}

	case reflect.Chan:
		r := new(Chan)
		r.zero = z
		r.cKeys = CandKeys(ckeystr)

		r.body = make(chan T)
		go func(body chan T) {
			for {
				// this will always attempt to pull at least one value
				val, ok := rChan.Recv()
				if !ok {
					break
				}
				body <- val.Interface()
			}
			close(body)
		}(r.body)

		// ensure minimal candidate keys
		if len(r.cKeys) == 0 {
			// perform a lazy distinct
			r.body = distinct(r.body)

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = defaultKeys(r.zero)

			// change the body to use a distinct channel instead of an assumed
			// distinct channel
		}

	case reflect.Slice:
		r := new(Slice)
		r.zero = z
		r.cKeys = CandKeys(ckeystr)

		// ensure minimal candidate keys
		if len(r.cKeys) == 0 {
			// do a greedy distinct if the data is already in memory
			m := map[interface{}]struct{}{}
			for i := 0; i < rval.Len(); i++ {
				m[rval.Index(i).Interface()] = struct{}{}
			}
			r.body = make([]interface{}, len(m))
			i = 0
			for k, _ := range m {
				r.body[i] = k
				i++
			}

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = defaultKeys(r.zero)

			// change the body to use a distinct channel instead of an assumed
			// distinct channel
		} else {
			r.body = make([]interface{}, rval.Len())
			for i := 0; i < rval.Len(); i++ {
				r.body[i] = rval.Index(i).Interface()
			}

		}

	default:
		panic(fmt.Sprintf("unrecognized relation kind: %v", rval.Kind()))
	}

	// we might want to check the candidate keys for validity here?
	orderCandidateKeys(r.cKeys)
	return r
}

// Heading is a slice of column name:type pairs
func Heading(r Relation) []Attribute {
	Names, Types := namesAndTypes(reflect.TypeOf(r.Zero()))
	h := make([]Attribute, len(Names))
	for i := 0; i < len(Names); i++ {
		h[i] = Attribute{Names[i], Types[i]}
	}
	return h
}

// Deg returns the degree of the relation
func Deg(r Relation) int {
	return len(r.Heading())
}

// Card returns the cardinality of the relation
// note: this consumes the values of the relation's tuples and can be an
// expensive operation.  We might want per-relation implementation of this?
func Card(r Relation) (i int) {
	tups := make(chan T)
	go r.Tuples(tups)
	for _ = range tups {
		i++
	}
	return
}

// The following methods generate relation expressions, also called queries.
// They are exported types because that way clients of the rel library can
// implement their own query reordering, if they want.

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func Project(r1 Relation, z2 T) ProjectExpr {
	return ProjectExpr{r1, z2}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// the
func Restrict(r Relation, p Predicate) RestrictExpr {
	f := reflect.ValueOf(p)
	subd := f.In(0)
	return RestrictExpr{r, p, subd}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?
func Rename(r Relation, z2 T) RenameExpr {
	return RenameExpr{r, z2}
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
func Join(r1, r2 Relation) JoinExpr {
	return JoinExpr{r1, r2}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func GroupBy(r Relation, t2, vt T, gfcn func(chan interface{}) interface{}) GroupByExpr {
	return GroupByExpr{r, t2, vt, gfcn}
}

// ThetaJoin creates a new relation by performing a theta-join on the inputs
// p should be a func (tup1 T, tup2 T) bool which when given a subdomain of
// the left relation and a subdomain of the right relation, returns a true
// if the combination should be included in the resulting relation.
func ThetaJoin(r1, r2 Relation, p Theta) ThetaJoinExpr {
	return ThetaJoinExpr{r1, r2, p}
}

// additional derived functions
// SemiDiff(r2 Relation) Relation
// SemiJoin(r2 Relation) Relation
// GroupBy(gtyp interface{}, vtyp interface{}, gfunc) Relation

// probably want to add non-Relational functions like
// Update
// Insert
// some kind of ordering?

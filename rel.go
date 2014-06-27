// relational interface definition, and methods defined on that interface.

package rel

import (
	"fmt"

	"reflect"
	"strings"
)

// Relation is a set of tuples with named attributes.
// See "A Relational Model of Data for Large Shared Data Banks" by Codd and
// "An introduction to Database Systems" by Date for the background for
// relational algebra.
type Relation interface {
	// Zero is the zero value for the tuple.  It provides a blank tuple,
	// similar to how a zero value is defined in the reflect package.  It will
	// have default values for each of its fields.
	Zero() interface{}

	// CKeys is the set of candidate keys for the Relation.  They will be
	// sorted from smallest key size to largest, and each key will be
	// sorted alphabetically.
	CKeys() CandKeys

	// TupleChan takes a channel with the same element type as Zero, and
	// concurrently sends the results of the relational operation over it.  You
	// should check the Err function before invoking this function to determine
	// if you can expect results, because if the relational operation is
	// malformed, then the TupleChan function will do nothing.  It returns a
	// cancel chan<- struct{} which can be used to terminate the calculations
	// before they have completed, by closing the cancel channel.  See
	// http://blog.golang.org/pipelines - specifically the section on explicit
	// cancellation - for a more in depth examination of how to use it.
	//
	// If you provide a non channel input or a channel with an element type
	// that does not match the Zero type, this method will result in a non nil
	// Err() return.
	TupleChan(interface{}) (cancel chan<- struct{})

	// TODO(jonlawlor): not sure if these would be useful...
	//TupleSlice(interface{}) ???
	//TupleMap(interface{}) ???

	// primatives of relational algebra

	// unary primatives

	// Project reduces the set of attributes used in the tuples of the
	// relation to a new given type, z2.
	//
	// If the input attributes z2 are not a subset of the attributes of the
	// source relation, then the resulting Relation will have non nill Err().
	Project(z2 interface{}) Relation

	// Restrict reduces the set of tuples in the relation to only those where
	// the predicate evaluates to true.
	//
	// If the input Predicate depends on attributes that do not exist in the
	// source relation, then the Relation result will have non nill Err().
	Restrict(p Predicate) Relation

	// Rename changes the names of attributes in a relation.  The new names
	// should be provided in the same order as the corresponding old names.
	//
	// If the input is not a struct, or if it has a different size than the
	// source's Zero, then the Relation result of this method will have non
	// nil Err().
	Rename(z2 interface{}) Relation

	// Map applies an input map function to each of the tuples in the source
	// relation.  Because map can transform any of the attributes in the
	// tuples, you have to provide a new set of candidate keys to this
	// operation.
	//
	// If the input candidate keys contain attributes that do not exist in
	// the resulting relation, or if the input mfcn is not a function, or if
	// it does not take tuples that are a subdomain of the source relation's,
	// or if it does not result in tuples, then the resulting Relation will
	// have non-nil Err().
	Map(mfcn interface{}, ckeystr [][]string) Relation

	// binary primatives

	// Union combines two relations into one relation, using a set union
	// operation.  If the two relations do not have the same Zero type, then
	// the resulting Relation will have a non nil Err().
	Union(r2 Relation) Relation

	// Diff removes values from one relation that match values in another.  If
	// the two relations do not have the same Zero type, then the resulting
	// Relation will have a non nil Err().
	Diff(r2 Relation) Relation

	// TODO(jonlawlor): implement SemiDiff

	// Join combines two relations by combining tuples between the two if the
	// tuples have identical values in the attributes that share the same
	// names.  This is also called an "equi-join" or natural join.  It is a
	// generalization of set intersection.  The second input, z3, should be a
	// blank structure with the attributes that the join will return.
	//
	// If z3 is not a struct, or if it contains attributes that do not exist
	// in the source relations, then the Err() field will be set.
	Join(r2 Relation, z3 interface{}) Relation

	// non relational but still useful

	// GroupBy provides arbitary aggregation of the tuples in the source
	// relation.  The
	// t2 is the resulting tuple type, gfcn is a function which takes as input
	// a channel of a subdomain of the tuples in the source relation, and then
	// produces result tuples that are a subdomain of the t2 tuple.  The
	// attributes that are in t2 that are not a part of the result tuples must
	// also exist in the source relation's tuples, and they are used to
	// determine unique groups.
	//
	// If t2 is not a blank example tuple struct, or if gfcn is not a function
	// which takes as input a channel with element type a subdomain of the
	// source relation, or if the result of the function is not a tuple
	// subdomain of t2, then the Err() result will be set.
	GroupBy(t2, gfcn interface{}) Relation

	// String provides a short relational algebra representation of the
	// relation.  It is particularly useful to determine which rewrite
	// rules have been applied.
	String() string

	// GoString provides a string which represents the tuples and the way
	// they are being transformed in go.  The result should be a string of
	// valid go code that will replicate the results of the input Relation.
	GoString() string

	// Err returns the first error that was encountered while either creating
	// the Relation, during a parent's evaluation, or during its own
	// evaluation.  There are two times when the Err() result may be non-nil:
	// either immediately after construction, or during the evaluation of the
	// TupleChan method.  If the Err() method does not return nil, then the
	// TupleChan method will never return any tuples, and further relational
	// operations will not be evaluated.
	Err() error
}

// New creates a new Relation from a []struct, map[struct] or chan struct.
//
// If the input v is not a []struct, map[struct], or a chan struct, then this
// function will panic.  If the input candidate keys are not a subset of the
// attributes of the input relation, then the Err() method of the resulting
// Relation will be non-nil.
func New(v interface{}, ckeystr [][]string) Relation {

	// depending on the type of the input, we represent a relation in different
	// types of relation.
	rbody := reflect.ValueOf(v)

	switch rbody.Kind() {
	case reflect.Map:
		e := rbody.Type().Key()
		z := reflect.Indirect(reflect.New(e)).Interface()

		r := new(mapLiteral)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			// maps are already distinct on the key, so the Map relation type
			// does not have a sourceDistinct field.  Maps are probably the
			// most natural way of storing relations.

			// all relations have a candidate key of all of their attributes, or
			// a non zero subset if the relation is not dee or dum
			r.cKeys = DefaultKeys(z)
		} else {
			// convert from [][]string to CandKeys
			r.cKeys = String2CandKeys(ckeystr)
		}
		r.zero = z
		// we might want to check the candidate keys for validity here?
		OrderCandidateKeys(r.cKeys)
		return r

	case reflect.Chan:
		e := rbody.Type().Elem()
		z := reflect.Indirect(reflect.New(e)).Interface()

		r := new(chanLiteral)
		r.rbody = rbody // TODO(jonlawlor): check direction
		if len(ckeystr) == 0 {
			r.cKeys = DefaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = String2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z
		// we might want to check the candidate keys for validity here?
		OrderCandidateKeys(r.cKeys)
		return r

	case reflect.Slice:
		e := rbody.Type().Elem()
		z := reflect.Indirect(reflect.New(e)).Interface()

		r := new(sliceLiteral)
		r.rbody = rbody
		if len(ckeystr) == 0 {
			r.cKeys = DefaultKeys(z)
			// note that even zero degree relations need to be distinct
		} else {
			r.cKeys = String2CandKeys(ckeystr)
			r.sourceDistinct = true
		}

		r.zero = z

		// we might want to check the candidate keys for validity here?
		OrderCandidateKeys(r.cKeys)
		return r
	default:
		panic(fmt.Sprintf("unrecognized relation kind: %v", rbody.Kind()))
	}
}

// Heading is a slice containing the attributes of the input Relation.
func Heading(r Relation) []Attribute {
	return FieldNames(reflect.TypeOf(r.Zero()))
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

// GoString returns a string representation of the relation that should
// evaluate to a relation with identical tuples as the source.
func GoString(r Relation) string {
	return goStringTabTable(r)
}

// Deg returns the degree of the relation
func Deg(r Relation) int {
	return len(Heading(r))
}

// Card returns the cardinality of the relation
// note: this consumes the values of the relation's tuples and can be an
// expensive operation.
func Card(r Relation) (i int) {
	z := r.Zero()
	e := reflect.TypeOf(z)

	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e), 0)
	_ = r.TupleChan(body.Interface())

	for {
		if _, ok := body.Recv(); !ok {
			break
		}
		i++
	}
	return
}

// TODO(jonlawlor): move error checking to the relational methods, to avoid
// rechecking during query rewrite.

// NewProject creates a new relation expression with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
// It should be used to implement new Relations.
func NewProject(r1 Relation, z2 interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	att2 := FieldNames(reflect.TypeOf(z2))
	err := EnsureSubDomain(att2, Heading(r1))
	if Deg(r1) == len(att2) && err == nil {
		// projection is a no op
		return r1
	} else {
		return &projectExpr{r1, z2, err}
	}
}

// NewRestrict creates a new relation expression with less than or equal cardinality.
// p has to be a predicate of a subdomain of the input relation.
// It should be used to implement new Relations.
func NewRestrict(r1 Relation, p Predicate) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	err := EnsureSubDomain(p.Domain(), Heading(r1))
	return &restrictExpr{r1, p, err}
}

// NewRename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation.
// It should be used to implement new Relations.
func NewRename(r1 Relation, z2 interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	d1 := Deg(r1)
	d2 := len(FieldNames(reflect.TypeOf(z2)))

	if d1 != d2 {
		return &renameExpr{r1, z2, &DegreeError{d1, d2}}
	}
	return &renameExpr{r1, z2, nil}
}

// NewUnion creates a new relation by unioning the bodies of both inputs.
// It should be used to implement new Relations.
func NewUnion(r1, r2 Relation) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	err := EnsureSameDomain(Heading(r1), Heading(r2))
	return &unionExpr{r1, r2, err}
}

// NewDiff creates a new relation by set minusing the two inputs.
// It should be used to implement new Relations.
func NewDiff(r1, r2 Relation) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	err := EnsureSameDomain(Heading(r1), Heading(r2))
	return &diffExpr{r1, r2, err}
}

// NewJoin creates a new relation by performing a natural join on the inputs.
// It should be used to implement new Relations.
func NewJoin(r1, r2 Relation, zero interface{}) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	if r2.Err() != nil {
		// don't bother building the relation and just return the original
		return r2
	}
	err := EnsureSubDomain(FieldNames(reflect.TypeOf(zero)), append(Heading(r1), Heading(r2)...))
	return &joinExpr{r1, r2, zero, err}
}

// NewGroupBy creates a new relation by grouping and applying a user defined
// function.  It should be used to implement new Relations.
func NewGroupBy(r1 Relation, t2, gfcn interface{}) Relation {
	// TODO(jonlawlor): add a code path which chooses map if the groupings
	// are unique.
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	// gfcn has to be a function with one input, and one output, where the
	// input is a subdomain of r1, and where the output is a subdomain of t2.
	rgfcn := reflect.ValueOf(gfcn)
	err, intup, outtup := EnsureGroupFunc(rgfcn.Type(), r1.Zero(), t2)
	return &groupByExpr{r1, t2, intup, outtup, rgfcn, err}
}

// NewMap creates a new relation by applying a function to tuples in the
// source. It should be used to implement new Relations.
func NewMap(r1 Relation, mfcn interface{}, ckeystr [][]string) Relation {
	if r1.Err() != nil {
		// don't bother building the relation and just return the original
		return r1
	}
	// determine the type of the returned tuples
	rmfcn := reflect.ValueOf(mfcn)
	err, intup, outtup := EnsureMapFunc(rmfcn.Type(), r1.Zero())
	z2 := reflect.Indirect(reflect.New(outtup)).Interface()

	if len(ckeystr) == 0 {
		// all relations have a candidate key of all of their attributes, or
		// a non zero subset if the relation is not dee or dum
		return &mapExpr{r1, z2, intup, outtup, rmfcn, DefaultKeys(z2), false, err}
	}
	return &mapExpr{r1, z2, intup, outtup, rmfcn, String2CandKeys(ckeystr), true, err}
}

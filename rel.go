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
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"text/tabwriter"
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

// fieldIndex is used to map between attributes in different relations
// that have the same name
type fieldIndex struct {
	i int
	j int
}

// Predicate is the type of func that takes a tuple and returns bool
// and is used for restrict & update
type Predicate func(tup T) bool

// theta is the type of func used as a predicate in theta-joins
type Theta func(tup1 T, tup2 T) bool

// Still need to hammer out exactly what should be in the Relation
// interface.  Some of the relational operations can be constructed
// from others, but their performance may be worse.  The important
// detail is that if the operations are a part of the interface then
// they have to have the syntax RelVar.Op(Params), while if the
// operations are defined on the interface then they can have the form
// Op(RelVar, Params).  A common go idiom is to implement that kind of
// function as a function, and then if the caller completes a different
// interface, just use that form, and otherwise have a "default"
// implementation.

// It might be much better to use a channel to hold all of the
// data within a relation.  Then as we perform operations, the
// operations will transform the channels, which should result
// in some interesting concurrency.

// Relation has similar meaning to tables in SQL
type Relation interface {
	// Heading is a slice of column name:type pairs
	Heading() []Attribute

	// Zero is the zero value for the tuple
	Zero() T

	// CKeys is the set of candidate keys for the Relation
	CKeys() CandKeys

	// Degree; the number of attributes
	Deg() int

	// Cardinality; the number of tuples in the body
	Card() int

	// Tuples takes a channel of interface and keeps sending
	// the tuples in the relation over the channel.
	// should this be allowed to consume the internal channel?
	Tuples(chan T) // does this channel need a direction?

	// the rest of these functions should be moved to functions of one or more relation

	// Project the relation to new type
	//Project(T)

	// Union
	//Union(r2 Relation)

	// SetDiff
	//SetDiff(r2 Relation)

	// copy the relation so that new values don't consume old ones
	Copy() Relation

	// theta join
	// JoinTheta(r2 Relation, Theta) Relation

	// natural join
	// Join(r2 Relation) Relation

	// Rename
	// Rename(???) Relation

	// additional derived functions
	// SemiDiff(r2 Relation) Relation
	// SemiJoin(r2 Relation) Relation
	// GroupBy(gtyp interface{}, vtyp interface{}, gfunc) Relation

	// probably want to add non-Relational functions like
	// Update
	// Insert
	// some kind of ordering?

	// these are not relational but they are sure nice to have
	GoString() string
	String() string
}

// interfaceHeading returns a map from column names to types for an
// input interface
func interfaceHeading(i T) []Attribute {
	Names, Types := namesAndTypes(reflect.TypeOf(i))
	h := make([]Attribute, len(Names))
	for i := 0; i < len(Names); i++ {
		h[i] = Attribute{Names[i], Types[i]}
	}
	return h
}

func namesAndTypes(e reflect.Type) ([]string, []reflect.Type) {
	n := e.NumField()
	names := make([]string, n)
	types := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		names[i] = f.Name
		types[i] = f.Type
	}
	return names, types
}

func orderCandidateKeys(ckeys CandKeys) {
	// first go through each set of keys and alphabetize
	// this is used to compare sets of candidate keys
	for _, ck := range ckeys {
		sort.Strings(ck)
	}

	// then sort by length so that smaller keys are first
	sort.Sort(ckeys)
}

// definitions for the candidate key sorting
func (cks CandKeys) Len() int {
	return len(cks)
}
func (cks CandKeys) Swap(i, j int) {
	cks[i], cks[j] = cks[j], cks[i]
}
func (cks CandKeys) Less(i, j int) bool {
	return len(cks[i]) < len(cks[j]) // note this is smallest to largest
}

// distinct changes an interface channel to a channel of unique interfaces
func distinct(b1 chan T) b2 chan T {
	m := make(map[interface{}]struct{})
	b2 := make(chan T)
	go func() {
		for v := range b1 {
			if _, dup := m[v]; !dup {
				m[v] = struct{}{}
				b2 <- v
			}
		}
		close(b2)
	}()
	return
}

// checkCandidateKeys checks the set of candidate keys
// this ensures that the names of the keys are all in the attributes
// of the relation
func checkCandidateKeys(ckeys CandKeys, cn []string) (err error) {
	// TODO(jonlawlor) cannonicalize these somehow
	names := make(map[string]struct{})
	for _, n := range cn {
		names[n] = struct{}{}
	}
	for _, ck := range ckeys {
		if len(ck) == 0 {
			// note that this doesn't fire if ckeys is also empty
			// but that is by design
			err = fmt.Errorf("empty candidate key not allowed")
			return
		}
		for _, k := range ck {
			_, keyFound := names[k]
			if !keyFound {
				err = fmt.Errorf("prime attribute not found: %s", k)
				return
			}
		}
	}
	return
}

// goStringTabTable is makes a gostring out of a given relation
// this isn't a method of relation (and then named GoString()) because
// go doesn't allow methods to be defined on interfaces.
func goStringTabTable(r Relation) string {
	// use a buffer to write to and later turn into a string
	s := bytes.NewBufferString("rel.New([]struct {\n")

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape)

	// create struct slice type information
	// TODO(jonlawlor): include tags?
	for _, att := range r.Heading() {
		fmt.Fprintf(w, "\t\xff%s\xff\t\xff%v\xff\t\n", att.Name, att.Type)
	}
	w.Flush()
	s.WriteString("}{\n")

	// write the body
	//TODO(jonlawlor): see if buffering the channel improves performance
	tups := make(chan interface{})
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := r.Deg()
	for tup := range tups {
		rtup := reflect.ValueOf(tup)
		// this part might be replacable with some workers that
		// convert tuples to strings
		fmt.Fprintf(w, "\t{")
		for j := 0; j < deg; j++ {
			f := rtup.Field(j)
			switch f.Kind() {
			case reflect.String:
				fmt.Fprintf(w, "\xff%q\xff,\t", f)
			case reflect.Bool:
				fmt.Fprintf(w, "%t,\t", f.Bool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// TODO(jonlawlor): I'm not sure all have to be enumerated
				fmt.Fprintf(w, "%d,\t", f.Int())
			case reflect.Float32, reflect.Float64:
				// TODO(jonlawlor): is there a general float type to use here?
				fmt.Fprintf(w, "%g,\t", f.Float())
			default:
				fmt.Fprintf(w, "\xff%v\xff,\t", f)
			}
		}
		fmt.Fprintf(w, "},\n")
	}

	w.Flush()
	s.WriteString("})")
	return s.String()
}

// stringTabTable is makes a gostring out of a given relation
// this isn't a method of relation (and then named GoString()) because
// go doesn't allow methods to be defined on interfaces.
func stringTabTable(r Relation) string {

	// use a buffer to write to and later turn into a string
	s := new(bytes.Buffer)

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	// align elements to the right as well
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape|tabwriter.AlignRight)

	//TODO(jonlawlor): not sure how to create the vertical seps like:
	//+---------+---------+---------+
	// which should go in between each of the sections of heading and body
	// also, I don't know where the candidate keys should go.  Date
	// does an underline but they can be overlapping, and I am not sure
	// that unicode allows arbitrary nesting of underlines?  In any case
	// it is not possible to arrange arbitrary candidate keys to be
	// adjacent.

	// create heading information
	for _, att := range r.Heading() {
		fmt.Fprintf(w, "\t\xff%s\xff\t\xff%v\xff\t\n", att.Name, att.Type)
	}

	// write the body
	//TODO(jonlawlor): see if buffering the channel improves performance
	tups := make(chan interface{})
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := r.Deg()
	for tup := range tups {
		rtup := reflect.ValueOf(tup)
		// this part might be replacable with some workers that
		// convert tuples to strings
		for j := 0; j < deg; j++ {
			f := rtup.Field(j)
			switch f.Kind() {
			case reflect.String:
				fmt.Fprintf(w, "|\t \xff%s\xff ", f)
			case reflect.Bool:
				fmt.Fprintf(w, "|\t %t ", f.Bool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// TODO(jonlawlor): I'm not sure all have to be enumerated
				fmt.Fprintf(w, "|\t %d ", f.Int())
			case reflect.Float32, reflect.Float64:
				// TODO(jonlawlor): there may be another representation
				fmt.Fprintf(w, "|\t %g ", f.Float())
			default:
				fmt.Fprintf(w, "|\t \xff%v\xff ", f)
			}
		}
		fmt.Fprintf(w, "\t|\n")
	}

	w.Flush()
	return s.String()
}

// fieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func fieldMap(e1 reflect.Type, e2 reflect.Type) map[string]fieldIndex {
	// TODO(jonlawlor): we might want to exclude unexported fields?
	m := make(map[string]fieldIndex)
	for i := 0; i < e1.NumField(); i++ {
		n1 := e1.Field(i).Name
		// find the field location in the original tuples
		for j := 0; j < e2.NumField(); j++ {
			n2 := e2.Field(j).Name
			if n1 == n2 {
				m[n1] = fieldIndex{i, j}
				break
			}
		}
	}
	return m
}

// fieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func attributeMap(h1 []Attribute, h2 []Attribute) map[string]fieldIndex {
	m := make(map[string]fieldIndex)
	for i := 0; i < len(h1); i++ {
		n1 := h1[i].Name
		// find the field location in the other heading
		for j := 0; j < len(h2); j++ {
			if n1 == h2[j].Name {
				m[n1] = fieldIndex{i, j}
				break
			}
		}
	}
	return m
}

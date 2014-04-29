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
	"text/tabwriter"
)

// Attribute represents a name:type pair which defines the heading
// of the relation
type Attribute struct {
	Name string
	Type reflect.Type
}

// Predicate is the type of func that takes a tuple and returns bool
// and is used for restrict & update
type Predicate func(tup interface{}) bool

// theta is the type of func used as a predicate in theta-joins
type Theta func(tup1 interface{}, tup2 interface{}) bool

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

	// Degree; the number of attributes
	Deg() int

	// Cardinality; the number of tuples in the body
	Card() int

	// Tuples takes a channel of reflect.value and keeps sending
	// the tuples in the relation over the channel.
	Tuples(chan reflect.Value) // this channel needs a direction?

	// Restrict
	// Restrict(p Predicate) Relation

	// theta join
	// JoinTheta(r2 Relation, Theta) Relation

	// natural join
	// Join(r2 Relation) Relation

	// Project
	Project(t2 interface{}) (r2 Relation)

	// Rename
	// Rename(???) Relation

	// Union
	Union(r2 Relation) Relation

	// SetDiff
	SetDiff(r2 Relation) Relation

	// additional derived functions
	// SemiDiff(r2 Relation) Relation
	// SemiJoin(r2 Relation) Relation
	// GroupBy(gtyp interface{}, vtyp interface{}, gfunc) Relation

	// probably want to add non-Relational functions like
	// Update
	// Insert
	// some kind of ordering?

	// I'm not sure that including GoString and String is the right
	// way to do this.
	GoString() string
	String() string
}

// New creates a new Relation.
// it returns a Relation implemented using the Simple
// structure, which keeps Tuples in a slice of struct.  We may want to
// change this to be more flexible with now relations are represented.
func New(v interface{}, ckeys [][]string) (rel Simple, err error) {
	//TODO(jonlawlor): allow callers to provide different inputs,
	// like map[struct{...}]struct{} or chan struct{...} which could also
	// represent a relation, and also error out if we can't figure out
	// how to construct a relation from the input.
	// There should also be a way to construct a relation with an input
	// that you already know is distinct, so we don't have to ensure it
	// ourselves.

	e := reflect.TypeOf(v).Elem()
	cn, ct := namesAndTypes(e)
	b := make([]reflect.Value, 0, 0)
	if len(ckeys) == 0 {
		fmt.Println("no keyes")
		// all relations have a candidate key of all of their
		// attributes
		ckeys = append(ckeys, []string{})
		copy(ckeys[0], cn)
		b = distinct(v, e)
	} else {
		err = checkCandidateKeys(ckeys, cn)
		if err != nil {
			return
		}
		// we don't have to perform a distinct because we are
		// assuming that the input is.
		bs := reflect.ValueOf(v)
		c := bs.Len()
		b = make([]reflect.Value, c, c)
		for i := 0; i < c; i++ {
			b[i] = bs.Index(i)
		}
	}
	rel = Simple{cn, ct, b, ckeys, e}

	return
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

// distinct changes an interface struct slice to a slice of unique reflect.Values
func distinct(v interface{}, e reflect.Type) []reflect.Value {
	m := reflect.MakeMap(reflect.MapOf(e, reflect.TypeOf(struct{}{})))
	b := reflect.ValueOf(v)
	c := b.Len()
	blank := reflect.ValueOf(struct{}{})
	for i := 0; i < c; i++ {
		m.SetMapIndex(b.Index(i), blank)
	}

	// I tried using the append unique method descibed in
	// http://blog.golang.org/profiling-go-programs but it took much longer
	// under benchmarks.  It may be because the comparison has to be done
	// using reflect.DeepEqual(x.Interface(), y.Interface())
	// I suspect that for large slices the map implementation is more efficent
	// because it has lower time complexity.

	// from tests it seems like the order of reflect.MapKeys() is
	// not randomized, (as of go 1.2) but we can't rely on that.
	// TODO(jonlawlor): change the string tests to be order independent.
	return m.MapKeys()
}

// checkCandidateKeys checks the set of candidate keys
// this ensures that the names of the keys are all in the attributes
// of the relation
func checkCandidateKeys(ckeys [][]string, cn []string) (err error) {
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
	tups := make(chan reflect.Value)
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := r.Deg()
	for tup := range tups {
		// this part might be replacable with some workers that
		// convert tuples to strings
		fmt.Fprintf(w, "\t{")
		for j := 0; j < deg; j++ {
			f := tup.Field(j)
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
	tups := make(chan reflect.Value)
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := r.Deg()
	for tup := range tups {
		// this part might be replacable with some workers that
		// convert tuples to strings
		for j := 0; j < deg; j++ {
			f := tup.Field(j)
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
func fieldMap(e1 reflect.Type, e2 reflect.Type) map[string]struct {
	i int
	j int
} {
	// TODO(jonlawlor): we might want to exclude unexported fields?
	m := make(map[string]struct {
		i int
		j int
	})
	for i := 0; i < e1.NumField(); i++ {
		n1 := e1.Field(i).Name
		// find the field location in the original tuples
		for j := 0; j < e2.NumField(); j++ {
			n2 := e2.Field(j).Name
			if n1 == n2 {
				m[n1] = struct {
					i int
					j int
				}{i, j}
				break
			}
		}
	}
	return m
}

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

// Relation has similar meaning to tables in SQL
type Relation interface {
	// the heading is a slice of column name:type pairs
	Heading() []Attribute

	Deg() int  // Degree; the number of attributes
	Card() int // Cardinality; the number of tuples in the body

	Tuples(chan reflect.Value)
}

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

// New creates a new Relation.
// it returns a Relation implemented using the Simple
// structure, which keeps Tuples in a slice of struct.
func New(v interface{}, ckeys [][]string) (rel Simple, err error) {
	//TODO(jonlawlor): allow callers to provide different inputs,
	// like map[struct{...}]struct{} or chan struct{...} which could also
	// represent a relation, and also error out if we can't figure out
	// how to construct a relation from the input

	e := reflect.TypeOf(v).Elem()
	n := e.NumField()
	cn := make([]string, n)
	ct := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		cn[i] = f.Name
		ct[i] = f.Type
	}
	if len(ckeys) == 0 {
		// all relations have a candidate key of all of their
		// attributes
		// TODO(jonlawlor): this feels like a clumsy way of doing this.
		ckeys = append(ckeys, []string{})
		copy(ckeys[0], cn)
	}
	err = checkCandidateKeys(ckeys, cn)
	if err != nil {
		return
	}
	rel = Simple{cn, ct, distinct(v, e), ckeys, e}

	return
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

// need to make a Map or Apply function which will evaluate a function
// on the output of the Tuples t chan

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
				fmt.Fprintf(w, "%d,\t", f.Int())
			case reflect.Float32, reflect.Float64:
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

func (r Simple) String() string {
	return stringTabTable(r)
}

func stringTabTable(r Relation) string {

	// use a buffer to write to and later turn into a string
	s := new(bytes.Buffer)

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	// align elements to the right as well
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape&tabwriter.AlignRight)

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
				fmt.Fprintf(w, "|\t %d ", f.Int())
			case reflect.Float32, reflect.Float64:
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

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subset of the current tuple's
// type.  We can't use a slice of strings because go can't construct
// arbitrary types through reflection.
//func (r1 Simple) Project(t2 interface{}) (r2 Relation) {
// figure out which of the candidate keys (if any) to keep.
// only the keys that only have attributes in the new type are
// valid.  If we do have any keys that are still valid, then
// we don't have to perform distinct on the body again.

// take each of the Tuples, transform them into the new type
// and append them to the new tuple body

// construct the returned relation
//}

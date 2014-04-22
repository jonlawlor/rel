// rel is a package that implements relational algebra
// the relational algebra here follows in the footsteps of "Database in
// Depth" by C. J. Date.  Therefore all terminology should be the same as
// used in that book.  There are some notable differences from SQL - the
// biggest of which is that all Relations are automatically distinct.
//
// The current implementation:
// It makes heavy use of reflection, but should provide some interesting
// ways of programming in go.  Because it uses so much reflection, it is
// difficult to implement in an idiomatic way.
//
// general outline of stuff todo:
// implement relations with structs that hold slices of structs, and also
// include some type information.  Then, each of the relational operators:
// projectrename (in place of just project and rename), restrict,
// thetajoin, setdiff, union, groupby, update, assignment, etc. will all
// be implemented with some reflection.

package rel

import (
	"bytes"
	"fmt"
	"reflect"
	"text/tabwriter"
)

type Attribute struct {
	Name string
	Type reflect.Type
}

// Relation has similar meaning to tables in SQL
type Relation interface {
	// the headding is a set of column name:type pairs
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

	// I wish there was a more precise way of representing this
	Body interface{}

	// set of candidate keys
	CKeys [][]string
}

// New creates a new Relation.
// it returns a Relation implemented using the Simple
// structure, which keeps Tuples in a slice of struct.
func New(v interface{}, ckeys [][]string) (rel Relation, err error) {
	e := reflect.TypeOf(v).Elem()
	n := e.NumField()
	cn := make([]string, n)
	ct := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		cn[i] = f.Name
		ct[i] = f.Type
	}
	err = checkCandidateKeys(ckeys, cn)
	if err != nil {
		return
	}
	rel = Simple{cn, ct, v, ckeys}
	return
}

// checkCandidateKeys checks the set of candidate keys
// this ensures that the names of the keys are all in the attributes
// of the relation
func checkCandidateKeys(ckeys [][]string, cn []string) (err error) {
	for _, ck := range ckeys {
		if len(ck) == 0 {
			err = fmt.Errorf("null candidate key not allowed")
			return
		}
		for _, k := range ck {
			keyFound := false
			for _, n := range cn {
				if k == n {
					keyFound = true
					break
				}
			}
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
	return reflect.ValueOf(r.Body).Len()
}

// Tuples sends each tuple in the relation to a channel
func (r Simple) Tuples(t chan reflect.Value) {
	c := r.Card()
	b := reflect.ValueOf(r.Body)

	go func() {
		defer close(t)
		for i := 0; i < c; i++ {
			t <- b.Index(i)
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
func (r Simple) String() string {
	return tabTable(r)
}

func tabTable(r Relation) string {
	// actually, is it possible to use the go fmt tool code to do
	// this for us?  It seems like a better way.
	// well, it would be except it doesn't align fields within
	// slices of struct.  Oh well.

	// use a buffer to write to and later turn into a string
	s := bytes.NewBufferString("rel.New([]struct {\n")

	w := new(tabwriter.Writer)
	w.Init(s, 1, 1, 1, ' ', 0)

	// create struct type
	for _, att := range r.Heading() {
		fmt.Fprintf(w, "\t%s\t%v\t\n", att.Name, att.Type)
	}
	w.Flush()
	s.WriteString("}{\n")

	// write the body
	//TODO(jonlawlor): see if buffering the channel improves performance
	tups := make(chan reflect.Value)
	r.Tuples(tups)

	deg := r.Deg()
BodyLoop:
	for {
		// pull one of the tuples
		select {
		case tup, ok := <-tups:
			if !ok {
				break BodyLoop
			}
			fmt.Fprintf(w, "\t{")
			for j := 0; j < deg; j++ {
				f := tup.Field(j)
				switch f.Kind() {
				case reflect.String:
					fmt.Fprintf(w, "%q,\t", f)
				case reflect.Bool:
					fmt.Fprintf(w, "%t,\t", f.Bool())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fmt.Fprintf(w, "%d,\t", f.Int())
				case reflect.Float32, reflect.Float64:
					fmt.Fprintf(w, "%g,\t", f.Float())
				default:
					fmt.Fprintf(w, "%v,\t", f)
				}
			}
			fmt.Fprintf(w, "},\n")
		}
	}

	w.Flush()
	s.WriteString("})")
	return s.String()
}

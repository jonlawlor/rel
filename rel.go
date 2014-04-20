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
	"fmt"
	"reflect"
	"strings"
)

// Tuple has similar meaning to rows in SQL
type Tuple interface{}

// Relation has similar meaning to tables in SQL
type Relation interface {
	// the headding is a set of column name:type pairs
	Heading() map[string]reflect.Type
	Deg() int  // Degree; the number of attributes
	Card() int // Cardinality; the number of tuples in the body
}

type relStruct struct {
	// Names & Types constitute the heading of the relation
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to
	// use the methods defined on a particular struct.
	Names []string
	Types []reflect.Type

	// I wish there was a more precise way of representing this
	Body interface{}
}

func New(v interface{}) (rel relStruct) {
	e := reflect.TypeOf(v).Elem()
	n := e.NumField()
	cn := make([]string, n)
	ct := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		cn[i] = f.Name
		ct[i] = f.Type
	}
	rel = relStruct{cn, ct, v}
	return
}

func (r relStruct) Deg() int {
	return len(r.Names)
}
func (r relStruct) Card() int {
	return reflect.ValueOf(r.Body).Len()
}

// Heading returns a map from column names to types
func (r relStruct) Heading() (h map[string]reflect.Type) {
	for i := 0; i < len(r.Names); i++ {
		h[r.Names[i]] = r.Types[i]
	}
	return
}

// String returns a text representation of the Relation
func (r relStruct) String() (str string) {
	// figure out the string representation of each value
	// within each of the tuples, and build up a 2d slice of
	// strings with that representation.  While this is going
	// on, figure out how long each of the strings are.

	// actually, is it possible to use the go fmt tool code to do
	// this for us?  It seems like a better way.
	// well, it would be except it doesn't align fields within
	// slices of strings.  Oh well.
	
	b := reflect.ValueOf(r.Body)
	
	d := r.Deg()
	c := r.Card()
	
	
	// create the heading
	hdr := make([]string,2*d,2*d+1)
	hdrsz := make([]int, 2, 2)
	for i:= 0; i < d; i++ {
		hdr[i*2] = r.Names[i]
		if hdrsz[0] < len(r.Names[i]) {
			hdrsz[0] = len(r.Names[i])
		}
		hdr[i*2+1] = fmt.Sprintf("%v",r.Types[i])
	}
	padStrings(hdr,hdrsz,"\t"," ","\n",d,2)
	hdr = append([]string{"Relation([]struct {\n"}, hdr...)
	hdr = append(hdr,"}{\n")
	
	numElem := d * c
	// each element in the slice represents one of the struct's
	// rows * columns.  Columns increment first, so [0] is the 
	// first row's first column, [1] is the first row's second
	// column, and so on.
	
	// str is the string of each element in the relation,
	// num is the maximum number of characters in the fmt.Sprintf
	// representation when delimiters and escape characters are
	// included.
	s := make([]string, numElem, numElem+1)
	n := make([]int, d, d)
	for i := 0; i < c; i++ {
		for j := 0; j < d; j++ {
			// flatten 2d to 1d
			ndx := i*d + j
			f := b.Index(i).Field(j)
			switch f.Kind() {
			case reflect.String:
				s[ndx] = fmt.Sprintf("\"%s\"",f)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				s[ndx] = fmt.Sprintf("%d",f.Int())
			case reflect.Float32, reflect.Float64:
				s[ndx] = fmt.Sprintf("%f",f.Float())
			default:
				s[ndx] = fmt.Sprintf("%v",f)
					
			}
			// we end up taking len(s[ndx]) 3 times this way
			// if it turns out to be slow in the profiler we
			// might want to put it into another slice
			if n[j] < len(s[ndx]) {
				n[j] = len(s[ndx])
			}
		} 
	}
	
	// go back through each of the strings and pad with spaces and
	// add newlines and tab whitespace
	
	if len(n) > 0 {
		// the first column has an extra tab and {
		n[0] = n[0]+2 
	}
	
	padStrings(s,n,"\t{",",","},\n",c,d)
	s = append(s, "})")
	str = strings.Join(append(hdr,s...),"")
	return
}

func padStrings(s []string, n []int, start string, delim string, end string, c int, d int) () {
	for i := 0; i < c; i++ {
		// each line begins with a tab + {
		s[i*d] = fmt.Sprintf("%s%s",start,s[i*d])
		for j := 0; j < d-1; j++ {
			ndx := i*d + j
			s[ndx] = fmt.Sprintf("%s%s%s",s[ndx],delim,strings.Repeat(" ",n[j] - len(s[ndx]) + 1))
		}
		// each line ends with a newline but doesn't require pad
		s[(i + 1) * d - 1] = fmt.Sprintf("%s%s",s[(i + 1) * d - 1],end)
	}
}
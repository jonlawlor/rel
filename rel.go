
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
	"reflect"
	"fmt"
)

// Tuple has similar meaning to rows in SQL
type Tuple interface{}

// Relation has similar meaning to tables in SQL
type Relation interface {
	// the headding is a set of column name:type pairs
	Heading() map[string]reflect.Type
	Deg() int // Degree; the number of attributes
	Card() int // Cardinality; the number of tuples in the body
}

type relStruct struct {
	// Names & Types constitute the heading of the relation
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to 
	// use the methods defined on a particular struct.
	Names  []string  
	Types  []reflect.Type
	
	// I wish there was a more precise way of representing this
	Body interface{}
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
func (r relStruct) String() string {
	// figure out the string representation of each value
	// within each of the tuples, and build up a 2d slice of 
	// strings with that representation.  While this is going
	// on, figure out how long each of the strings are
	
	// placeholder
	return fmt.Sprintf("%v",r)
	
	// go back through each of the strings and pad with spaces
	
	// create a human readable heading
	
	// construct the text with some extra ascii to make it easier
	// to understand
}
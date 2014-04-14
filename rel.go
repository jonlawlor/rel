
// rel is a package that implements relational algebra
// It makes heavy use of reflection, but should provide some interesting
// ways of programming in go.  Because it uses so much reflection, it is
// difficult to implement in an idiomatic way.
// 
// current plan:
// implement relations with structs that hold slices of structs, and also
// include some type information.  Then, each of the relational operators:
// projectrename (in place of just project and rename), restrict,
// thetajoin, setdiff, union, groupby, update, assignment, etc. will all
// be implemented with some reflection.

package rel

import {
	"reflect"
}

type Relation interface {
	ProjectRename(colnamemap interface{}) (Relation)
}
package rel

import (
	"testing"
)

// tests for setdiff op
func TestSetDiff(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel1, _ := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	exRel2, _ := New(exampleRel2(100), [][]string{[]string{"Foo"}})

	r1 := exRel1.Union(exRel2)
	if r1.Card() != exRel2.Card() {
		t.Errorf("exRel1.Union(exRel2).Card() = %d, want \"%d\"", r1.Card(), exRel2.Card())

	}
	return
}

//TODO(jonlawlor): add in benchmarks

package rel

import (
	"testing"
)

// tests for setdiff op
func TestSetDiff(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRel2(100), [][]string{[]string{"Foo"}})

	r1 := SetDiff(exRel1, exRel2)
	if Card(r1) != 0 {
		t.Errorf("Card(exRel1.SetDiff(exRel2)) = %d, want \"%d\"", Card(r1), 0)

	}
	return
}

//TODO(jonlawlor): add in benchmarks

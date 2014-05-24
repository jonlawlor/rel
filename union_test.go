package rel

import (
	"testing"
)

// tests union op
func TestUnion(t *testing.T) {
	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRel2(100), [][]string{[]string{"Foo"}})

	r1 := Union(exRel1, exRel2)
	if Card(r1) != Card(exRel2) {
		t.Errorf("Card(Union(exRel1, exRel2)) = %d, want \"%d\"", Card(r1), Card(exRel2))
	}
	return
}

//TODO(jonlawlor): add in benchmarks

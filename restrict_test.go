package rel

import (
	"testing"
)

// tests for restrict op
func TestRestrict(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel, _ := New(exampleRel2(10), [][]string{[]string{"Foo"}})

	r1 := exRel.Restrict(func(i interface{}) bool {
		return true
	})
	if r1.Card() != exRel.Card() {
		t.Errorf("identity restrict has card = %d, want \"%d\"", r1.Card(), exRel.Card())
	}

	r2 := exRel.Restrict(func(i interface{}) bool {
		return false
	})
	if r2.Card() != 0 {
		t.Errorf("restrict with false Predicate has card = %d, want \"%d\"", r2.Card(), 0)
	}

	r3 := exRel.Restrict(func(i interface{}) bool {
		return i.(exTup2).Foo > 5
	})
	if r3.Card() != 4 {
		t.Errorf("restrict has card = %d, want \"%d\"", r3.Card(), 4)
	}

	return
}

//TODO(jonlawlor): add in benchmarks

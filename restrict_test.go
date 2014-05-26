package rel

import (
	"testing"
)

// tests for restrict op
func TestRestrict(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	r1 := Restrict(exRel, AdHoc{func(i struct{}) bool {
		return true
	}})

	if Card(r1) != Card(exRel) {
		t.Errorf("identity restrict has card = %d, want \"%d\"", Card(r1), Card(exRel))
	}

	r2 := Restrict(exRel, AdHoc{func(i struct{}) bool {
		return false
	}})
	if Card(r2) != 0 {
		t.Errorf("restrict with false Predicate has card = %d, want \"%d\"", Card(r2), 0)
	}

	r3 := Restrict(exRel, AdHoc{func(i struct{ Foo int }) bool {
		return i.Foo > 5
	}})
	if Card(r3) != 4 {
		t.Errorf("restrict has card = %d, want \"%d\"", Card(r3), 4)
	}

	return
}

func BenchmarkRestrictIdent(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always true)
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	pred := AdHoc{func(i struct{}) bool {
		return true
	}}
	r1 := Restrict(exRel, pred)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

func BenchmarkRestrictZero(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// zero predicate (always false)
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	pred := AdHoc{func(i struct{}) bool {
		return false
	}}
	r1 := Restrict(exRel, pred)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

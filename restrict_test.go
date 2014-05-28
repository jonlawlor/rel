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

// These Native functions are useful to determine what kind of overhead
// reflection is incuring.

func BenchmarkRestrictIdentNative(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always true)
	exRel := exampleRelSlice2(10)
	Pred := func(exTup2) bool {
		return true
	}

	NativeTups := func(t chan exTup2) {
		go func() {
			for _, tup := range exRel {
				t <- tup
			}
			close(t)
		}()
		return
	}

	NativeRestrict := func(src chan exTup2, res chan exTup2) {
		go func() {
			for tup := range src {
				if Pred(tup) {
					res <- tup
				}
			}
			close(res)
		}()
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := make(chan exTup2)
		NativeTups(src)
		res := make(chan exTup2)
		NativeRestrict(src, res)
		for _ = range res {
		}
	}
}

func BenchmarkRestrictZeroNative(b *testing.B) {
	exRel := exampleRelSlice2(10)
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always false)

	Pred := func(exTup2) bool {
		return false
	}

	NativeTups := func(t chan exTup2) {
		go func() {
			for _, tup := range exRel {
				t <- tup
			}
			close(t)
		}()
		return
	}

	NativeRestrict := func(src chan exTup2, res chan exTup2) {
		go func() {
			for tup := range src {
				if Pred(tup) {
					res <- tup
				}
			}
			close(res)
		}()
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := make(chan exTup2)
		NativeTups(src)
		res := make(chan exTup2)
		NativeRestrict(src, res)
		for _ = range res {
		}
	}
}

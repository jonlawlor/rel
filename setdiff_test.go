package rel

import (
	"testing"
)

// tests for setdiff op
func TestSetDiff(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(100), [][]string{[]string{"Foo"}})

	r1 := SetDiff(exRel1, exRel2)
	if Card(r1) != 0 {
		t.Errorf("Card(exRel1.SetDiff(exRel2)) = %d, want \"%d\"", Card(r1), 0)

	}
	return
}

func BenchmarkSetDiff(b *testing.B) {
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	r1 := SetDiff(exRel1, exRel2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 0 tuples (1 dupe each)
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

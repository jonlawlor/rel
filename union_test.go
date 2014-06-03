package rel

import (
	"testing"
)

// tests union op
func TestUnion(t *testing.T) {
	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(100), [][]string{[]string{"Foo"}})

	r1 := exRel1.Union(exRel2)
	if Card(r1) != Card(exRel2) {
		t.Errorf("Card(exRel1.Union(exRel2)) = %d, want \"%d\"", Card(r1), Card(exRel2))
	}
	return
}

func BenchmarkUnion(b *testing.B) {
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	r1 := exRel1.Union(exRel2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 10 tuples (1 dupe each)
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

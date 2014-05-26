package rel

import (
	"testing"
)

// test creation of relations, including tests to determine the cost of
// representing slices of structs as relations instead of native.

// type of the example relations
type exTup2 struct {
	Foo int
	Bar string
}

// exampleRelSlice2 creates an example relation body using slice
// with given cardinality and degree 2.
func exampleRelSlice2(c int) []exTup2 {
	recs := make([]exTup2, c)
	for i := 0; i < c; i++ {
		recs[i] = exTup2{i, "test"}
	}
	return recs
}

// exampleRelMap2 creates an example relation body using map
// with given cardinality and degree 2.
func exampleRelMap2(c int) map[exTup2]struct{} {
	recs := make(map[exTup2]struct{}, c)
	for i := 0; i < c; i++ {
		recs[exTup2{i, "test"}] = struct{}{}
	}
	return recs
}

// exampleRelSlice2 creates an example relation body using chan
// with given cardinality and degree 2.
func exampleRelChan2(c int) chan exTup2 {
	recs := make(chan exTup2)
	go func() {
		for i := 0; i < c; i++ {
			recs <- exTup2{i, "test"}
		}
		close(recs)
	}()
	return recs
}

// test the degrees
func TestDeg(t *testing.T) {
	fix := []struct {
		name string
		in   int
		out  int
	}{
		{"suppliers", Deg(suppliers), 4},
		{"parts", Deg(parts), 5},
		{"orders", Deg(orders), 3},
	}
	for i, dt := range fix {
		if dt.in != dt.out {
			t.Errorf("%d. %s.Deg() => %d, want %d", i, dt.name, dt.in, dt.out)
		}
	}
}

func TestCard(t *testing.T) {
	fix := []struct {
		name string
		in   int
		out  int
	}{
		{"suppliers", Card(suppliers), 5},
		{"parts", Card(parts), 6},
		{"orders", Card(orders), 12},
	}
	for i, dt := range fix {
		if dt.in != dt.out {
			t.Errorf("%d. %s.Card() => %d, want %d", i, dt.name, dt.in, dt.out)
		}
	}
}

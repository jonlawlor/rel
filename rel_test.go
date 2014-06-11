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

// test of rel.New using a slice of tuples
func TestNewSlice(t *testing.T) {
	r := New(exampleRelSlice2(1), [][]string{[]string{"Foo"}})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}
}

//test of rel.New using a map of tuples
func TestNewMap(t *testing.T) {
	r := New(exampleRelMap2(1), [][]string{[]string{"Foo"}})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}

}

// test of rel.New using a chan of tuples
func TestNewChan(t *testing.T) {
	r := New(exampleRelChan2(1), [][]string{[]string{"Foo"}})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}
}

// test of rel.New using a non distinct slice of tuples
func TestNewNonDistinctSlice(t *testing.T) {
	r := New(exampleRelSlice2(1), [][]string{})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}
}

//test of rel.New using a non distinct map of tuples
func TestNewNonDistinctMap(t *testing.T) {
	r := New(exampleRelMap2(1), [][]string{})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}

}

// test of rel.New using a non distinct chan of tuples
func TestNewNonDistinctChan(t *testing.T) {
	r := New(exampleRelChan2(1), [][]string{})
	if c := Card(r); c != 1 {
		t.Errorf("rel.New has Card() => %v, want %v", c, 1)
	}
}

package rel

import (
	"testing"
)

// TestMatrixExample is an example of sparse matrix algebra implemented in
// relational algebra.
func TestMatrixExample(t *testing.T) {

	type matrixElem struct {
		R int
		C int
		V float64
	}
	type multElemA struct {
		R  int
		M  int
		VA float64
	}
	type multElemB struct {
		M  int
		C  int
		VB float64
	}
	type multElemC struct {
		R  int
		C  int
		M  int
		VA float64
		VB float64
	}
	//type multRes struct {
	//	R int
	//	C int
	//	M int
	//	V float64
	//}
	//mapMult := func(tup multElemC) multRes {
	//	return multRes{tup.R, tup.C, tup.M, tup.VA * tup.VB}
	//}
	type groupTup struct {
		VA float64
		VB float64
	}
	type valTup struct {
		V float64
	}
	groupAdd := func(val <-chan groupTup) valTup {
		res := valTup{}
		for vi := range val {
			res.V += vi.VA * vi.VB
		}
		return res
	}

	// representation of a matrix:
	//  1 2
	//  3 4
	A := New([]matrixElem{
		{1, 1, 1.0},
		{1, 2, 2.0},
		{2, 1, 3.0},
		{2, 2, 4.0},
	}, [][]string{[]string{"R", "C"}})

	// representation of a matrix:
	//  4 17
	//  9 17
	B := New([]matrixElem{
		{1, 1, 4.0},
		{1, 2, 17.0},
		{2, 1, 9.0},
		{2, 2, 17.0},
	}, [][]string{[]string{"R", "C"}})

	C := A.Rename(multElemA{}).Join(B.Rename(multElemB{}), multElemC{}).
		GroupBy(matrixElem{}, groupAdd)

	expectRes := New([]matrixElem{
		{1, 1, 22},
		{1, 2, 51},
		{2, 1, 48},
		{2, 2, 119},
	}, [][]string{})

	if Card(C.Diff(expectRes)) != 0 || Card(expectRes.Diff(C)) != 0 {
		t.Errorf("matrix multiply has result => %v, want (ignore order) %v", C.GoString(), expectRes.GoString())
	}

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

// test creation of relations, including tests to determine the cost of
// representing slices of structs as relations instead of native.

// type of the example tuples in relations
type exTup2 struct {
	Foo int
	Bar string
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

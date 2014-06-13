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

func TestMatrixExample(t *testing.T) {
	// this should be changed to be a text example maybe.  It is a work in progress.
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
	type multRes struct {
		R int
		C int
		M int
		V float64
	}
	mapMult := func(tup T) T {
		if v, ok := tup.(multElemC); ok {
			return multRes{v.R, v.C, v.M, v.VA * v.VB}
		} else {
			return multRes{}
		}
	}
	type valTup struct {
		V float64
	}
	groupAdd := func(val <-chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.V += v.V
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

	C := A.Rename(multElemA{}).Join(B.Rename(multElemB{}), multElemC{}).Map(mapMult, multRes{}, [][]string{[]string{"R", "C", "M"}}).GroupBy(matrixElem{}, valTup{}, groupAdd)

	expectStr := `rel.New([]struct {
 R int     
 C int     
 V float64 
}{
 {1, 1, 22,  },
 {1, 2, 51,  },
 {2, 1, 48,  },
 {2, 2, 119, },
})`

	if cStr := C.GoString(); cStr != expectStr {
		t.Errorf("matrix multiply has string representation => %v, want %v", cStr, expectStr)
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

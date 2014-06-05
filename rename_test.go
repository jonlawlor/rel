package rel

import (
	"testing"
)

// tests for Rename
func TestRename(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	type r1tup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := orders.Rename(r1tup{})
	if GoString(r1) != GoString(orders) {
		t.Errorf("orders.Rename(PNO, SNO, Qty) = \"%s\", want \"%s\"", GoString(r1), GoString(orders))

	}
	type r2tup struct {
		PartNo   int
		SupplyNo int
		Quantity int
	}

	r2 := orders.Rename(r2tup{})
	r2GoString := `rel.New([]struct {
 PartNo   int 
 SupplyNo int 
 Quantity int 
}{
 {1, 1, 300, },
 {1, 2, 200, },
 {1, 3, 400, },
 {1, 4, 200, },
 {1, 5, 100, },
 {1, 6, 100, },
 {2, 1, 300, },
 {2, 2, 400, },
 {3, 2, 200, },
 {4, 2, 200, },
 {4, 4, 300, },
 {4, 5, 400, },
})`
	if GoString(r2) != r2GoString {
		t.Errorf("orders.Rename(PartNo, SupplyNo, Quantity) = \"%s\", want \"%s\"", GoString(r2), r2GoString)
	}
	return
}

func BenchmarkRename(b *testing.B) {
	type r2tup struct {
		PartNo   int
		SupplyNo int
		Quantity int
	}
	r1 := orders.Rename(r2tup{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 12 tuples
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

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

	r1 := Rename(orders, r1tup{})
	if r1.GoString() != orders.GoString() {
		t.Errorf("orders.Rename(PNO, SNO, Qty) = \"%s\", want \"%s\"", r1.GoString(), orders.GoString())

	}
	type r2tup struct {
		PartNo   int
		SupplyNo int
		Quantity int
	}

	r2 := Rename(orders, r2tup{})
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
	if r2.GoString() != r2GoString {
		t.Errorf("orders.Rename(PartNo, SupplyNo, Quantity) = \"%s\", want \"%s\"", r2.GoString(), r2GoString)

	}

	return
}

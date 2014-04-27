package rel

import (
	"testing"
)

// tests for projection conversion

func TestProject(t *testing.T) {
	//
	
	
	// TODO(jonlawlor): replace with table driven test?
	type r1tup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := orders.Project(r1tup{})
	if r1.GoString() != orders.GoString() {
		t.Errorf("orders.Project(PNO, SNO, Qty) = \"%s\", want \"%s\"", r1.GoString(), orders.GoString())

	}
	type r2tup struct {
		PNO int
		SNO int
	}

	r2 := orders.Project(r2tup{})
	r2GoString := `rel.New([]struct {
 PNO int 
 SNO int 
}{
 {1, 1, },
 {1, 2, },
 {1, 3, },
 {1, 4, },
 {1, 5, },
 {1, 6, },
 {2, 1, },
 {2, 2, },
 {3, 2, },
 {4, 2, },
 {4, 4, },
 {4, 5, },
})`
	if r2.GoString() != r2GoString {
		t.Errorf("orders.Project(PNO, SNO) = \"%s\", want \"%s\"", r2.GoString(), r2GoString)

	}
	
	type r3tup struct {
		PNO int
		Qty int
	}
	r3 := orders.Project(r3tup{})
	if r3.Deg() != 2 || r3.Card() != 10 {
		t.Errorf("orders.Project(PNO, Qty) has Deg %d, Card %d, want Deg %d, Card %d", r3.Deg(), r3.Card(), 2, 10)

	}
	return
}

package rel

import (
	"testing"
)

// tests for projection conversion

func TestProject(t *testing.T) {
	//
	type r1tup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := orders.Project(r1tup{})
	if r1.GoString() != orders.GoString() {
		t.Errorf("orders.Project(PNO, SNO, Qty) = \"%s\", want \"%s\"", r1.GoString(), orders.GoString())

	}
	/*type r2tup struct {
		PNO int
		SNO int
	}

	r2 := orders.Project(r2tup{})
	if r2.GoString() != orders.GoString() {
		t.Errorf("orders.Project(PNO, SNO) = \"%s\", want \"%s\"", r2.GoString(), orders.GoString())

	}
	type r3tup struct {
		PNO int
		Qty int
	}

	r3 := orders.Project(r3tup{})
	if r3.GoString() != orders.GoString() {
		t.Errorf("orders.Project(PNO, Qty) = \"%s\", want \"%s\"", r3.GoString(), orders.GoString())

	}
	*/
	return
}

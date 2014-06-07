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
	// test the degrees, cardinality, and string representation
	type upperCaseTup struct {
		PNO int
		SNO int
		QTY int
	}

	rel := orders.Rename(upperCaseTup{})
	type distinctTup struct {
		PNO int
		SNO int
	}
	type nonDistinctTup struct {
		PNO int
		QTY int
	}
	type titleCaseTup struct {
		Pno int
		Sno int
		Qty int
	}
	type joinTup struct {
		PNO    int
		SNO    int
		QTY    int
		SName  string
		Status int
		City   string
	}
	type groupByTup struct {
		PNO int
		QTY int
	}
	type valTup struct {
		QTY int
	}
	groupFcn := func(val chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.QTY += v.QTY
		}
		return res
	}
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 3, 6},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 12},
		{rel.Project(nonDistinctTup{}), "π{PNO, QTY}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 10},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, QTY}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 3, 12},
		{rel.SetDiff(orders.Rename(upperCaseTup{})), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) − ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 0},
		{rel.Union(orders.Rename(upperCaseTup{})), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) ∪ ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.Join(suppliers, joinTup{}), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)).GroupBy({PNO, QTY}, {QTY})", 2, 4},
	}

	for i, tt := range relTest {
		str := tt.rel.String()
		deg := Deg(tt.rel)
		card := Card(tt.rel)
		if str != tt.expectString {
			t.Errorf("%d has String() => %v, want %v", i, str, tt.expectString)
		}
		if deg != tt.expectDeg {
			t.Errorf("%d %s has Deg() => %v, want %v", i, tt.expectString, deg, tt.expectDeg)
		}
		if card != tt.expectCard {
			t.Errorf("%d %s has Card() => %v, want %v", i, tt.expectString, card, tt.expectCard)
		}
	}
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

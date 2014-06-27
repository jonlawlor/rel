package rel

import (
	"fmt"
	"github.com/jonlawlor/rel/att"
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

	r1 := orders().Rename(r1tup{})
	if GoString(r1) != GoString(orders()) {
		t.Errorf("orders.Rename(PNO, SNO, Qty) = \"%s\", want \"%s\"", GoString(r1), GoString(orders()))

	}
	type r2tup struct {
		PartNo   int
		SupplyNo int
		Quantity int
	}

	r2 := orders().Rename(r2tup{})
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

	rel := orders().Rename(upperCaseTup{})
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
	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.QTY += vi.QTY
		}
		return res
	}

	type mapRes struct {
		PNO  int
		SNO  int
		Qty1 int
		Qty2 int
	}
	mapFcn := func(tup1 upperCaseTup) mapRes {
		return mapRes{tup1.PNO, tup1.SNO, tup1.QTY, tup1.QTY * 2}
	}
	mapKeys := [][]string{
		[]string{"PNO", "SNO"},
	}
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.Restrict(att.Attribute("PNO").EQ(1)), "σ{PNO == 1}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 3, 6},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 12},
		{rel.Project(nonDistinctTup{}), "π{PNO, QTY}(ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 10},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.Diff(orders().Rename(upperCaseTup{})), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) − ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 0},
		{rel.Union(orders().Rename(upperCaseTup{})), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) ∪ ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.Join(suppliers(), joinTup{}), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{rel.GroupBy(groupByTup{}, groupFcn), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)).GroupBy({PNO, QTY}->{QTY})", 2, 4},
		{rel.Map(mapFcn, mapKeys), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)).Map({PNO, SNO, QTY}->{PNO, SNO, Qty1, Qty2})", 4, 12},
		{rel.Map(mapFcn, [][]string{}), "ρ{PNO, SNO, QTY}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty)).Map({PNO, SNO, QTY}->{PNO, SNO, Qty1, Qty2})", 4, 12},
	}

	for i, tt := range relTest {
		if err := tt.rel.Err(); err != nil {
			t.Errorf("%d has Err() => %v", err)
			continue
		}
		if str := tt.rel.String(); str != tt.expectString {
			t.Errorf("%d has String() => %v, want %v", i, str, tt.expectString)
		}
		if deg := Deg(tt.rel); deg != tt.expectDeg {
			t.Errorf("%d %s has Deg() => %v, want %v", i, tt.expectString, deg, tt.expectDeg)
		}
		if card := Card(tt.rel); card != tt.expectCard {
			t.Errorf("%d %s has Card() => %v, want %v", i, tt.expectString, card, tt.expectCard)
		}
	}
	// test cancellation
	res := make(chan upperCaseTup)
	cancel := rel.TupleChan(res)
	close(cancel)
	select {
	case <-res:
		t.Errorf("cancel did not end tuple generation")
	default:
		// passed test
	}

	// test errors
	err := fmt.Errorf("testing error")
	rel1 := orders().Rename(upperCaseTup{}).(*renameExpr)
	rel1.err = err
	rel2 := orders().Rename(upperCaseTup{}).(*renameExpr)
	rel2.err = err
	res = make(chan upperCaseTup)
	_ = rel1.TupleChan(res)
	if _, ok := <-res; ok {
		t.Errorf("did not short circuit TupleChan")
	}
	errTest := []Relation{
		rel1.Project(distinctTup{}),
		rel1.Restrict(att.Not(att.Attribute("PNO").EQ(1))),
		rel1.Union(rel2),
		rel.Union(rel2),
		rel1.Diff(rel2),
		rel.Diff(rel2),
		rel1.Join(rel2, orderTup{}),
		rel.Join(rel2, orderTup{}),
		rel1.GroupBy(groupByTup{}, groupFcn),
		rel1.Map(mapFcn, mapKeys),
	}
	for i, errRel := range errTest {
		if errRel.Err() != err {
			t.Errorf("%d did not short circuit error", i)
		}
	}

	errRel := (&errorRel{upperCaseTup{}, 1, nil}).Rename(upperCaseTup{})
	if c := Card(errRel); c != 1 {
		t.Errorf("errored relation had Card() => %v, wanted %v", c, 1)
	}
}

func BenchmarkRename(b *testing.B) {
	type r2tup struct {
		PartNo   int
		SupplyNo int
		Quantity int
	}
	r1 := orders().Rename(r2tup{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 12 tuples
		t := make(chan r2tup)
		r1.TupleChan(t)
		for _ = range t {
		}
	}
}

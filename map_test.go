package rel

import (
	"fmt"
	"testing"
)

// tests for map op
func TestMap(t *testing.T) {

	// test the degrees, cardinality, and string representation
	doubleQty := func(tup1 T) T {
		if v, ok := tup1.(orderTup); ok {
			return orderTup{v.PNO, v.SNO, v.Qty * 2}
		} else {
			return orderTup{}
		}
	}
	mapKeys := [][]string{
		[]string{"PNO", "SNO"},
	}

	rel := orders().Map(doubleQty, orderTup{}, mapKeys)
	type distinctTup struct {
		PNO int
		SNO int
	}
	type nonDistinctTup struct {
		PNO int
		Qty int
	}
	type titleCaseTup struct {
		Pno int
		Sno int
		Qty int
	}
	type joinTup struct {
		PNO    int
		SNO    int
		Qty    int
		SName  string
		Status int
		City   string
	}
	type groupByTup struct {
		PNO int
		Qty int
	}
	type valTup struct {
		Qty int
	}
	groupFcn := func(val <-chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.Qty += v.Qty
		}
		return res
	}
	type mapRes struct {
		PNO  int
		SNO  int
		Qty1 int
		Qty2 int
	}
	mapFcn := func(tup1 T) T {
		if v, ok := tup1.(orderTup); ok {
			return mapRes{v.PNO, v.SNO, v.Qty, v.Qty * 2}
		} else {
			return mapRes{}
		}
	}

	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty})", 3, 12},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}))", 3, 6},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}))", 2, 12},
		{rel.Project(nonDistinctTup{}), "π{PNO, Qty}(Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}))", 2, 10},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}))", 3, 12},
		{rel.SetDiff(orders()), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}) − Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Union(orders()), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}) ∪ Relation(PNO, SNO, Qty)", 3, 24},
		{rel.Join(suppliers(), joinTup{}), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}).GroupBy({PNO, Qty}, {Qty})", 2, 4},
		{rel.Map(mapFcn, mapRes{}, mapKeys), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 12},
		{rel.Map(mapFcn, mapRes{}, [][]string{}), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty}).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 12},
	}

	for i, tt := range relTest {
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
	res := make(chan T)
	cancel := rel.Tuples(res)
	close(cancel)
	select {
	case <-res:
		t.Errorf("cancel did not end tuple generation")
	default:
		// passed test
	}
	// test errors
	err := fmt.Errorf("testing error")
	r1 := orders().Map(doubleQty, orderTup{}, mapKeys).(*MapExpr)
	r1.err = err
	r2 := orders().Map(doubleQty, orderTup{}, mapKeys).(*MapExpr)
	r2.err = err
	res = make(chan T)
	_ = r1.Tuples(res)
	if _, ok := <-res; ok {
		t.Errorf("%d did not short circuit Tuples")
	}
	errTest := []Relation{
		r1.Project(distinctTup{}),
		r1.Restrict(Not(Attribute("PNO").EQ(1))),
		r1.Rename(titleCaseTup{}),
		r1.Union(r2),
		rel.Union(r2),
		r1.SetDiff(r2),
		rel.SetDiff(r2),
		r1.Join(r2, orderTup{}),
		rel.Join(r2, orderTup{}),
		r1.GroupBy(groupByTup{}, valTup{}, groupFcn),
		r1.Map(mapFcn, mapRes{}, mapKeys),
	}
	for i, errRel := range errTest {
		if errRel.Err() != err {
			t.Errorf("%d did not short circuit error", i)
		}
	}
}

package rel

import (
	"fmt"
	"github.com/jonlawlor/rel/att"
	"testing"
)

// tests for projection
func TestGroupBy(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	type r1tup struct {
		PNO int
		Qty int
	}
	type valtup struct {
		Qty int
	}

	// a simple summation
	groupFcn := func(val <-chan valtup) valtup {
		res := valtup{}
		for vi := range val {
			res.Qty += vi.Qty
		}
		return res
	}
	wantRes := New([]r1tup{
		{4, 900},
		{1, 1300},
		{2, 700},
		{3, 200},
	}, [][]string{})
	r1 := orders().GroupBy(r1tup{}, groupFcn)
	if Card(r1.Diff(wantRes)) != 0 || Card(wantRes.Diff(r1)) != 0 {
		t.Errorf("orders.Groupby = \"%s\", want (ignore order) \"%s\"", r1.GoString(), wantRes.GoString())
	}

	type groupByTup1 struct {
		PNO    int
		PName  string
		Weight float64
		City   string
	}
	type valTup struct {
		Weight float64
	}
	weightSum := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Weight += vi.Weight
		}
		return res
	}

	rel := parts().GroupBy(groupByTup1{}, weightSum)
	type distinctTup struct {
		PNO   int
		PName string
	}
	type nonDistinctTup struct {
		PName string
		City  string
	}
	type titleCaseTup struct {
		Pno    int
		PName  string
		Weight float64
		City   string
	}

	type joinTup struct {
		PNO    int
		PName  string
		Weight float64
		City   string
		SNO    int
	}
	type groupByTup2 struct {
		City   string
		Weight float64
	}

	type mapRes struct {
		PNO     int
		PName   string
		Weight2 float64
	}
	mapFcn := func(tup1 groupByTup1) mapRes {
		return mapRes{tup1.PNO, tup1.PName, tup1.Weight / 2}
	}

	mapKeys := [][]string{
		[]string{"PNO"},
	}

	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight})", 4, 6},
		{rel.Restrict(att.Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 4, 1},
		{rel.Project(distinctTup{}), "π{PNO, PName}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 2, 6},
		{rel.Project(nonDistinctTup{}), "π{PName, City}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 2, 6},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, PName, Weight, City}/{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 4, 6},
		{rel.Diff(rel.Restrict(att.Attribute("Weight").LT(15.0))), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}) − σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 4, 3},
		{rel.Union(rel.Restrict(att.Attribute("Weight").LE(12.0))), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}) ∪ σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}))", 4, 6},
		{rel.Join(suppliers(), joinTup{}), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}) ⋈ Relation(SNO, SName, Status, City)", 5, 10},
		{rel.GroupBy(groupByTup2{}, weightSum), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}).GroupBy({City, Weight}->{Weight})", 2, 3},
		{rel.Map(mapFcn, mapKeys), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}).Map({PNO, PName, Weight, City}->{PNO, PName, Weight2})", 3, 6},
		{rel.Map(mapFcn, [][]string{}), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}->{Weight}).Map({PNO, PName, Weight, City}->{PNO, PName, Weight2})", 3, 6},
	}
	for i, tt := range relTest {
		if err := tt.rel.Err(); err != nil {
			t.Errorf("%d has Err() => %s", i, err.Error())
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
	res := make(chan groupByTup1)
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
	rel1 := parts().GroupBy(groupByTup1{}, weightSum).(*groupByExpr)
	rel1.err = err
	rel2 := parts().GroupBy(groupByTup1{}, weightSum).(*groupByExpr)
	rel2.err = err
	res = make(chan groupByTup1)
	_ = rel1.TupleChan(res)
	if _, ok := <-res; ok {
		t.Errorf("%d did not short circuit TupleChan")
	}
	errTest := []Relation{
		rel1.Project(distinctTup{}),
		rel1.Restrict(att.Not(att.Attribute("PNO").EQ(1))),
		rel1.Rename(titleCaseTup{}),
		rel1.Union(rel2),
		rel.Union(rel2),
		rel1.Diff(rel2),
		rel.Diff(rel2),
		rel1.Join(rel2, orderTup{}),
		rel.Join(rel2, orderTup{}),
		rel1.GroupBy(groupByTup2{}, groupFcn),
		rel1.Map(mapFcn, mapKeys),
	}
	for i, errRel := range errTest {
		if errRel.Err() != err {
			t.Errorf("%d did not short circuit error", i)
		}
	}
}
func BenchmarkGroupBy(b *testing.B) {
	type r1tup struct {
		PNO int
		Qty int
	}
	type valtup struct {
		Qty int
	}

	// a simple summation
	groupFcn := func(val <-chan valtup) valtup {
		res := valtup{}
		for vi := range val {
			res.Qty += vi.Qty
		}
		return res
	}
	r1 := orders().GroupBy(r1tup{}, groupFcn)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 4 tuples

		t := make(chan r1tup)
		r1.TupleChan(t)
		for _ = range t {
		}
	}
}

package rel

import (
	"fmt"
	"github.com/jonlawlor/rel/att"
	"testing"
)

// tests for join
func TestJoin(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	type restup struct {
		PNO    int     // from the parts & orders tables
		PName  string  // from the parts table
		Color  string  // from the parts table
		Weight float64 // from the parts table
		City   string  // from the parts table
		SNO    int     // from the orders table
		Qty    int     // from the orders table
	}

	r1 := parts().Join(orders(), restup{})
	wantString := `rel.New([]struct {
 PNO    int     
 PName  string  
 Color  string  
 Weight float64 
 City   string  
 SNO    int     
 Qty    int     
}{
 {1, "Nut",   "Red",   12, "London", 1, 300, },
 {1, "Nut",   "Red",   12, "London", 2, 200, },
 {1, "Nut",   "Red",   12, "London", 3, 400, },
 {1, "Nut",   "Red",   12, "London", 4, 200, },
 {1, "Nut",   "Red",   12, "London", 5, 100, },
 {1, "Nut",   "Red",   12, "London", 6, 100, },
 {2, "Bolt",  "Green", 17, "Paris",  1, 300, },
 {2, "Bolt",  "Green", 17, "Paris",  2, 400, },
 {3, "Screw", "Blue",  17, "Oslo",   2, 200, },
 {4, "Screw", "Red",   14, "London", 2, 200, },
 {4, "Screw", "Red",   14, "London", 4, 300, },
 {4, "Screw", "Red",   14, "London", 5, 400, },
})`

	if GoString(r1) != wantString {
		t.Errorf("Join(parts, orders, restup{}).GoString() = \"%s\", want \"%s\"", GoString(r1), wantString)
	}

	type joinTup1 struct {
		PNO    int
		PName  string
		Weight float64
		City   string
		SNO    int
		Qty    int
	}
	rel := parts().Join(orders(), joinTup1{})
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
		Sno    int
		Qty    int
	}

	type joinTup2 struct {
		PNO    int
		PName  string
		Weight float64
		City   string
		SNO    int
		Qty    int
		SName  string
		Status int
	}
	type groupByTup struct {
		City   string
		Weight float64
		Qty    int
	}
	type valTup struct {
		Weight float64
		Qty    int
	}
	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Weight += vi.Weight
			res.Qty += vi.Qty
		}
		return res
	}

	type mapRes struct {
		PNO     int
		TotalWt float64
	}
	mapFcn := func(tup1 joinTup1) mapRes {
		return mapRes{tup1.PNO, tup1.Weight * float64(tup1.Qty)}
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
		{rel, "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty)", 6, 12},
		{rel.Restrict(att.Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, PName, Color, Weight, City)) ⋈ σ{PNO == 1}(Relation(PNO, SNO, Qty))", 6, 6},
		{rel.Project(distinctTup{}), "π{PNO, PName}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 2, 4},
		{rel.Project(nonDistinctTup{}), "π{PName, City}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 2, 4},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, PName, Weight, City, Sno, Qty}/{PNO, PName, Weight, City, SNO, Qty}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 6, 12},
		{rel.SetDiff(rel.Restrict(att.Attribute("Weight").LT(15.0))), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) − σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(PNO, SNO, Qty)", 6, 3},
		{rel.Union(rel.Restrict(att.Attribute("Weight").LE(12.0))), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) ∪ σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(PNO, SNO, Qty)", 6, 12},
		{rel.Join(suppliers(), joinTup2{}), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)", 8, 4},
		{rel.GroupBy(groupByTup{}, groupFcn), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty).GroupBy({City, Weight, Qty}->{Weight, Qty})", 3, 3},
		{rel.Map(mapFcn, mapKeys), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty).Map({PNO, PName, Weight, City, SNO, Qty}->{PNO, TotalWt})", 2, 12}, // this is not actually distinct
		{rel.Map(mapFcn, [][]string{}), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty).Map({PNO, PName, Weight, City, SNO, Qty}->{PNO, TotalWt})", 2, 10},
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
	res := make(chan joinTup1)
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
	rel1 := parts().Join(orders(), joinTup1{}).(*joinExpr)
	rel1.err = err
	rel2 := parts().Join(orders(), joinTup1{}).(*joinExpr)
	rel2.err = err
	res = make(chan joinTup1)
	_ = rel1.TupleChan(res)
	if _, ok := <-res; ok {
		t.Errorf("%d did not short circuit TupleChan")
	}
	errTest := []Relation{
		rel1.Project(distinctTup{}),
		rel1.Rename(titleCaseTup{}),
		rel1.Union(rel2),
		rel.Union(rel2),
		rel1.SetDiff(rel2),
		rel.SetDiff(rel2),
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
}

func BenchmarkJoin(b *testing.B) {
	type restup struct {
		PNO    int     // from the parts & orders tables
		PName  string  // from the parts table
		Color  string  // from the parts table
		Weight float64 // from the parts table
		City   string  // from the parts table
		SNO    int     // from the orders table
		Qty    int     // from the orders table
	}

	r1 := parts().Join(orders(), restup{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 12 tuples
		t := make(chan restup)
		r1.TupleChan(t)
		for _ = range t {
		}
	}
}

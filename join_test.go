package rel

import (
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

	r1 := parts.Join(orders, restup{})
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
	rel := parts.Join(orders, joinTup1{})
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
	}
	type valTup struct {
		Qty    int
		Weight float64
	}
	groupFcn := func(val chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.Weight += v.Weight * float64(v.Qty)
		}
		return res
	}
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty)", 6, 12},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, PName, Color, Weight, City)) ⋈ σ{PNO == 1}(Relation(PNO, SNO, Qty))", 6, 6},
		{rel.Project(distinctTup{}), "π{PNO, PName}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 2, 12},
		{rel.Project(nonDistinctTup{}), "π{PName, City}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 2, 4},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, PName, Weight, City, Sno, Qty}/{PNO, PName, Weight, City, SNO, Qty}(Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty))", 6, 12},
		{rel.SetDiff(rel.Restrict(Attribute("Weight").LT(15.0))), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) − σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(PNO, SNO, Qty)", 6, 3},
		{rel.Union(rel.Restrict(Attribute("Weight").LE(12.0))), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) ∪ σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(PNO, SNO, Qty)", 6, 12},
		{rel.Join(suppliers, joinTup2{}), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)", 8, 4},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(PNO, PName, Color, Weight, City) ⋈ Relation(PNO, SNO, Qty).GroupBy({City, Weight}, {Qty, Weight})", 2, 3},
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

	r1 := parts.Join(orders, restup{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 12 tuples
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

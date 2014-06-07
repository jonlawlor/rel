package rel

import (
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
	groupFcn := func(val <-chan T) T {
		res := valtup{}
		for vi := range val {
			v := vi.(valtup)
			res.Qty += v.Qty
		}
		return res
	}
	wantString := `rel.New([]struct {
 PNO int 
 Qty int 
}{
 {4, 900,  },
 {1, 1300, },
 {2, 700,  },
 {3, 200,  },
})`
	r1 := orders.GroupBy(r1tup{}, valtup{}, groupFcn)
	if r1.GoString() != wantString {
		t.Errorf("orders.Groupby = \"%s\", want \"%s\"", r1.GoString(), wantString)
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
	weightSum := func(val <-chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.Weight += v.Weight
		}
		return res
	}

	rel := parts.GroupBy(groupByTup1{}, valTup{}, weightSum)
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
		Qty    int
	}
	type groupByTup2 struct {
		City   string
		Weight float64
	}
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight})", 4, 6},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 4, 1},
		{rel.Project(distinctTup{}), "π{PNO, PName}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 2, 6},
		{rel.Project(nonDistinctTup{}), "π{PName, City}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 2, 6},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, PName, Weight, City}/{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 4, 6},
		{rel.SetDiff(rel.Restrict(Attribute("Weight").LT(15.0))), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}) − σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 4, 3},
		{rel.Union(rel.Restrict(Attribute("Weight").LE(12.0))), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}) ∪ σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}))", 4, 6},
		{rel.Join(suppliers, joinTup{}), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}) ⋈ Relation(SNO, SName, Status, City)", 6, 10},
		{rel.GroupBy(groupByTup2{}, valTup{}, weightSum), "Relation(PNO, PName, Color, Weight, City).GroupBy({PNO, PName, Weight, City}, {Weight}).GroupBy({City, Weight}, {Weight})", 2, 3},
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
func BenchmarkGroupBy(b *testing.B) {
	type r1tup struct {
		PNO int
		Qty int
	}
	type valtup struct {
		Qty int
	}

	// a simple summation
	groupFcn := func(val <-chan T) T {
		res := valtup{}
		for vi := range val {
			v := vi.(valtup)
			res.Qty += v.Qty
		}
		return res
	}
	r1 := orders.GroupBy(r1tup{}, valtup{}, groupFcn)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 4 tuples

		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

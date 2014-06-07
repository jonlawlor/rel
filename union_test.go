package rel

import (
	"testing"
)

// tests union op
func TestUnion(t *testing.T) {
	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(100), [][]string{[]string{"Foo"}})

	r1 := exRel1.Union(exRel2)
	if Card(r1) != Card(exRel2) {
		t.Errorf("Card(exRel1.Union(exRel2)) = %d, want \"%d\"", Card(r1), Card(exRel2))
	}

	// test the degrees, cardinality, and string representation
	rel := orders.Restrict(Attribute("Qty").GE(300)).Union(orders.Restrict(Attribute("Qty").NE(200)))
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
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty))", 3, 8},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{Qty >= 300}(σ{PNO == 1}(Relation(PNO, SNO, Qty))) ∪ σ{Qty != 200}(σ{PNO == 1}(Relation(PNO, SNO, Qty)))", 3, 4},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(σ{Qty >= 300}(Relation(PNO, SNO, Qty))) ∪ π{PNO, SNO}(σ{Qty != 200}(Relation(PNO, SNO, Qty)))", 2, 8},
		{rel.Project(nonDistinctTup{}), "σ{Qty >= 300}(π{PNO, Qty}(Relation(PNO, SNO, Qty))) ∪ σ{Qty != 200}(π{PNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 7},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty)))", 3, 8},
		{rel.SetDiff(orders), "σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty)) − Relation(PNO, SNO, Qty)", 3, 0},
		{rel.Union(orders), "σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty)) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Join(suppliers, joinTup{}), "σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 7},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "σ{Qty >= 300}(Relation(PNO, SNO, Qty)) ∪ σ{Qty != 200}(Relation(PNO, SNO, Qty)).GroupBy({PNO, Qty}, {Qty})", 2, 3},
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

func BenchmarkUnion(b *testing.B) {
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	r1 := exRel1.Union(exRel2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 10 tuples (1 dupe each)
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

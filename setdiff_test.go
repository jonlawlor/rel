package rel

import (
	"testing"
)

// tests for setdiff op
func TestSetDiff(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(100), [][]string{[]string{"Foo"}})

	r1 := exRel1.SetDiff(exRel2)
	if Card(r1) != 0 {
		t.Errorf("Card(exRel1.SetDiff(exRel2)) = %d, want \"%d\"", Card(r1), 0)

	}

	// test the degrees, cardinality, and string representation
	rel := orders.Restrict(Attribute("Qty").NE(300)).SetDiff(orders.Restrict(Attribute("Qty").EQ(200)))
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
		{rel, "σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty))", 3, 5},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{Qty != 300}(σ{PNO == 1}(Relation(PNO, SNO, Qty))) − σ{Qty == 200}(σ{PNO == 1}(Relation(PNO, SNO, Qty)))", 3, 3},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)))", 2, 5},
		{rel.Project(nonDistinctTup{}), "π{PNO, Qty}(σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)))", 2, 4},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)))", 3, 5},
		{rel.SetDiff(orders), "σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)) − Relation(PNO, SNO, Qty)", 3, 0},
		{rel.Union(orders), "σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Join(suppliers, joinTup{}), "σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 4},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "σ{Qty != 300}(Relation(PNO, SNO, Qty)) − σ{Qty == 200}(Relation(PNO, SNO, Qty)).GroupBy({PNO, Qty}, {Qty})", 2, 3},
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
func BenchmarkSetDiff(b *testing.B) {
	exRel1 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	exRel2 := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	r1 := exRel1.SetDiff(exRel2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 0 tuples (1 dupe each)
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

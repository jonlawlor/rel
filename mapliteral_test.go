package rel

import (
	"reflect"
	"testing"
)

// tests & benchmarks for the rel.mapLiteral type

func tomapLiteral(r Relation) Relation {
	// construct a channel using reflection
	z := r.Zero()
	m := reflect.MakeMap(reflect.MapOf(reflect.TypeOf(z), reflect.TypeOf(struct{}{})))
	v := reflect.ValueOf(struct{}{})
	t := make(chan T)
	go r.Tuples(t)
	for tup := range t {
		m.SetMapIndex(reflect.ValueOf(tup), v)
	}
	return &mapLiteral{m, r.CKeys(), r.Zero(), nil}
}

// test the degrees, cardinality, and string representation
func TestmapLiteral(t *testing.T) {
	rel := tomapLiteral(orders)
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
		{rel, "Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, SNO, Qty))", 3, 6},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(Relation(PNO, SNO, Qty))", 2, 12},
		{rel.Project(nonDistinctTup{}), "π{PNO, Qty}(Relation(PNO, SNO, Qty))", 2, 10},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{rel.SetDiff(orders), "Relation(PNO, SNO, Qty) − Relation(PNO, SNO, Qty)", 3, 0},
		{rel.Union(orders), "Relation(PNO, SNO, Qty) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Join(suppliers, joinTup{}), "Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(PNO, SNO, Qty).GroupBy({PNO, Qty}, {Qty})", 2, 4},
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

func BenchmarkMapLiteralNewTinySimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelMap2(10)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewTinyNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelMap2(10)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewSmallSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelMap2(1000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewSmallNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelMap2(1000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewMediumSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelMap2(100000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewMediumNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelMap2(100000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewLargeSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelMap2(10000000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkMapLiteralNewLargeNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelMap2(10000000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}
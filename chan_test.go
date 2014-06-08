package rel

import (
	"reflect"
	"testing"
)

// tests & benchmarks for the rel.chanLiteral type

// unlike the rel.Map and rel.Slice type, this has to drain the resulting
// relation, otherwise there will be hanging go-routines.  It would be better
// if we could cancel, but that might require a different type of relation.

// this allows us to drain a channel so that the source goroutines finish
func drain(t chan exTup2) {
	for _ = range t {
	}
	return
}

func tochanLiteral(r Relation) Relation {
	// construct a channel using reflection
	z := r.Zero()
	ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(z)), 0)
	t := make(chan T)
	_ = r.Tuples(t)
	go func(b <-chan T) {
		for tup := range b {
			ch.Send(reflect.ValueOf(tup))
		}
		ch.Close()
	}(t)
	return &chanLiteral{ch, r.CKeys(), r.Zero(), true, nil}
}

func TestchanLiteral(t *testing.T) {
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
		{tochanLiteral(orders), "Relation(PNO, SNO, Qty)", 3, 12},
		{tochanLiteral(orders).Restrict(Attribute("PNO").EQ(1)), "σ{PNO == 1}(Relation(PNO, SNO, Qty))", 3, 6},
		{tochanLiteral(orders).Project(distinctTup{}), "π{PNO, SNO}(Relation(PNO, SNO, Qty))", 2, 12},
		{tochanLiteral(orders).Project(nonDistinctTup{}), "π{PNO, Qty}(Relation(PNO, SNO, Qty))", 2, 10},
		{tochanLiteral(orders).Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{tochanLiteral(orders).SetDiff(tochanLiteral(orders)), "Relation(PNO, SNO, Qty) − Relation(PNO, SNO, Qty)", 3, 0},
		{tochanLiteral(orders).Union(tochanLiteral(orders)), "Relation(PNO, SNO, Qty) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{tochanLiteral(orders).Join(tochanLiteral(suppliers), joinTup{}), "Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{tochanLiteral(orders).GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(PNO, SNO, Qty).GroupBy({PNO, Qty}, {Qty})", 2, 4},
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

func BenchmarkChanLiteralNewTinySimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelChan2(10)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewTinyNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelChan2(10)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewSmallSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelChan2(1000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewSmallNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelChan2(1000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewMediumSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelChan2(100000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewMediumNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelChan2(100000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewLargeSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelChan2(10000000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

func BenchmarkChanLiteralNewLargeNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelChan2(10000000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
	// channel cleanup
	b.StopTimer()
	drain(exRel)
}

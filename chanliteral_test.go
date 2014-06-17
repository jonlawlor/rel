package rel

import (
	"fmt"
	"github.com/jonlawlor/rel/att"
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

func toChanLiteral(r Relation, isDistinct bool) Relation {
	// construct a channel using reflection
	z := r.Zero()
	ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(z)), 0)
	t := make(chan interface{})
	_ = r.Tuples(t)
	go func(b <-chan interface{}) {
		for tup := range b {
			ch.Send(reflect.ValueOf(tup))
		}
		ch.Close()
	}(t)
	return &chanLiteral{ch, r.CKeys(), r.Zero(), isDistinct, nil}
}

func TestChanLiteral(t *testing.T) {
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
	groupFcn := func(val <-chan interface{}) interface{} {
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
	mapFcn := func(tup1 interface{}) interface{} {
		if v, ok := tup1.(orderTup); ok {
			return mapRes{v.PNO, v.SNO, v.Qty, v.Qty * 2}
		} else {
			return mapRes{}
		}
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
		{toChanLiteral(orders(), true), "Relation(PNO, SNO, Qty)", 3, 12},
		{toChanLiteral(orders(), true).Restrict(att.Not(att.Attribute("PNO").EQ(1))), "σ{!(PNO == 1)}(Relation(PNO, SNO, Qty))", 3, 6},
		{toChanLiteral(orders(), true).Project(distinctTup{}), "π{PNO, SNO}(Relation(PNO, SNO, Qty))", 2, 12},
		{toChanLiteral(orders(), true).Project(nonDistinctTup{}), "π{PNO, Qty}(Relation(PNO, SNO, Qty))", 2, 10},
		{toChanLiteral(orders(), true).Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))", 3, 12},
		{toChanLiteral(orders(), true).SetDiff(toChanLiteral(orders(), false)), "Relation(PNO, SNO, Qty) − Relation(PNO, SNO, Qty)", 3, 0},
		{toChanLiteral(orders(), true).Union(toChanLiteral(orders(), false)), "Relation(PNO, SNO, Qty) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{toChanLiteral(orders(), true).Join(toChanLiteral(suppliers(), false), joinTup{}), "Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)", 6, 11},
		{toChanLiteral(orders(), true).GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(PNO, SNO, Qty).GroupBy({PNO, Qty}, {Qty})", 2, 4},
		{toChanLiteral(orders(), true).Map(mapFcn, mapRes{}, mapKeys), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 12},
		{toChanLiteral(orders(), true).Map(mapFcn, mapRes{}, [][]string{}), "Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 12},
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
	r := toChanLiteral(orders(), true)
	res := make(chan interface{})
	cancel := r.Tuples(res)
	close(cancel)
	select {
	case <-res:
		t.Errorf("cancel did not end tuple generation")
	default:
		// passed test
	}

	// test non distinct & cancellation
	r = toChanLiteral(orders(), false)
	res = make(chan interface{})
	cancel = r.Tuples(res)
	close(cancel)
	select {
	case <-res:
		t.Errorf("cancel did not end tuple generation")
	default:
		// passed test
	}

	// test errors
	err := fmt.Errorf("testing error")
	r1 := new(chanLiteral)
	r1 = toChanLiteral(orders(), true).(*chanLiteral)
	r1.err = err
	r2 := new(chanLiteral)
	r2 = toChanLiteral(orders(), true).(*chanLiteral)
	r2.err = err
	res = make(chan interface{})
	_ = r1.Tuples(res)
	if _, ok := <-res; ok {
		t.Errorf("%d did not short circuit Tuples")
	}
	errTest := []Relation{
		r1.Project(distinctTup{}),
		r1.Restrict(att.Not(att.Attribute("PNO").EQ(1))),
		r1.Rename(titleCaseTup{}),
		r1.Union(r2),
		r.Union(r2),
		r1.SetDiff(r2),
		r.SetDiff(r2),
		r1.Join(r2, orderTup{}),
		r.Join(r2, orderTup{}),
		r1.GroupBy(groupByTup{}, valTup{}, groupFcn),
		r1.Map(mapFcn, mapRes{}, mapKeys),
	}
	for i, errRel := range errTest {
		if errRel.Err() != err {
			t.Errorf("%d did not short circuit error", i)
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

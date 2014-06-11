package rel

import (
	"fmt"
	"testing"
)

// tests for restrict op
func TestRestrict(t *testing.T) {

	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})

	var restrictTests = []struct {
		in  Relation
		out int
	}{
		{exRel.Restrict(AdHoc{func(i struct{}) bool { return true }}), 10},
		{exRel.Restrict(AdHoc{func(i struct{}) bool { return false }}), 0},
		{exRel.Restrict(AdHoc{func(i struct{ Foo int }) bool { return i.Foo > 5 }}), 4},
	}
	for _, tt := range restrictTests {
		c := Card(tt.in)
		if c != tt.out {
			t.Errorf("Card(%s) => %v, want %v", tt.in.GoString(), c, tt.out)
		}
	}

	// test the degrees, cardinality, and string representation
	rel := orders().Restrict(Attribute("Qty").GT(100))
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
	mapKeys := [][]string{
		[]string{"PNO", "SNO"},
	}

	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "σ{Qty > 100}(Relation(PNO, SNO, Qty))", 3, 10},
		{rel.Restrict(Attribute("PNO").EQ(1).And(Attribute("Qty").GT(200))), "σ{Qty > 100}(σ{(PNO == 1) && (Qty > 200)}(Relation(PNO, SNO, Qty)))", 3, 2},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(σ{Qty > 100}(Relation(PNO, SNO, Qty)))", 2, 10},
		{rel.Project(nonDistinctTup{}), "σ{Qty > 100}(π{PNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 9},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(σ{Qty > 100}(Relation(PNO, SNO, Qty)))", 3, 10},
		{rel.SetDiff(orders()), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) − Relation(PNO, SNO, Qty)", 3, 0},
		{rel.Union(orders()), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Join(suppliers(), joinTup{}), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 10},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "σ{Qty > 100}(Relation(PNO, SNO, Qty)).GroupBy({PNO, Qty}, {Qty})", 2, 4},
		{rel.Map(mapFcn, mapRes{}, mapKeys), "σ{Qty > 100}(Relation(PNO, SNO, Qty)).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 10},
		{rel.Map(mapFcn, mapRes{}, [][]string{}), "σ{Qty > 100}(Relation(PNO, SNO, Qty)).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})", 4, 10},
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
	r1 := orders().Restrict(Attribute("Qty").GT(100)).(*RestrictExpr)
	r1.err = err
	r2 := orders().Restrict(Attribute("Qty").GT(100)).(*RestrictExpr)
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

func BenchmarkRestrictIdent(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always true)
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	pred := AdHoc{func(i struct{}) bool {
		return true
	}}
	r1 := exRel.Restrict(pred)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

func BenchmarkRestrictZero(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// zero predicate (always false)
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	pred := AdHoc{func(i struct{}) bool {
		return false
	}}
	r1 := exRel.Restrict(pred)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

// These Native functions are useful to determine what kind of overhead
// reflection is incuring.  My measurements show Ident is ~2.5 times slower
// and Zero is ~4 times slower than native.

func BenchmarkRestrictIdentNative(b *testing.B) {
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always true)
	exRel := exampleRelSlice2(10)
	Pred := func(exTup2) bool {
		return true
	}

	NativeTups := func(t chan exTup2) {
		go func() {
			for _, tup := range exRel {
				t <- tup
			}
			close(t)
		}()
		return
	}

	NativeRestrict := func(src chan exTup2, res chan exTup2) {
		go func() {
			for tup := range src {
				if Pred(tup) {
					res <- tup
				}
			}
			close(res)
		}()
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := make(chan exTup2)
		NativeTups(src)
		res := make(chan exTup2)
		NativeRestrict(src, res)
		for _ = range res {
		}
	}
}

func BenchmarkRestrictZeroNative(b *testing.B) {
	exRel := exampleRelSlice2(10)
	// test the time it takes to pull all of the tuples after passing in an
	// identity predicate (always false)

	Pred := func(exTup2) bool {
		return false
	}

	NativeTups := func(t chan exTup2) {
		go func() {
			for _, tup := range exRel {
				t <- tup
			}
			close(t)
		}()
		return
	}

	NativeRestrict := func(src chan exTup2, res chan exTup2) {
		go func() {
			for tup := range src {
				if Pred(tup) {
					res <- tup
				}
			}
			close(res)
		}()
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := make(chan exTup2)
		NativeTups(src)
		res := make(chan exTup2)
		NativeRestrict(src, res)
		for _ = range res {
		}
	}
}

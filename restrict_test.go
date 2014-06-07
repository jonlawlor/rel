package rel

import (
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
	rel := orders.Restrict(Attribute("Qty").GT(100))
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
	groupFcn := func(val chan T) T {
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
		{rel, "σ{Qty > 100}(Relation(PNO, SNO, Qty))", 3, 10},
		{rel.Restrict(Attribute("PNO").EQ(1)), "σ{Qty > 100}(σ{PNO == 1}(Relation(PNO, SNO, Qty)))", 3, 4},
		{rel.Project(distinctTup{}), "π{PNO, SNO}(σ{Qty > 100}(Relation(PNO, SNO, Qty)))", 2, 10},
		{rel.Project(nonDistinctTup{}), "σ{Qty > 100}(π{PNO, Qty}(Relation(PNO, SNO, Qty)))", 2, 9},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(σ{Qty > 100}(Relation(PNO, SNO, Qty)))", 3, 10},
		{rel.SetDiff(orders), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) − Relation(PNO, SNO, Qty)", 3, 0},
		{rel.Union(orders), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) ∪ Relation(PNO, SNO, Qty)", 3, 12},
		{rel.Join(suppliers, joinTup{}), "σ{Qty > 100}(Relation(PNO, SNO, Qty)) ⋈ Relation(SNO, SName, Status, City)", 6, 10},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "σ{Qty > 100}(Relation(PNO, SNO, Qty)).GroupBy({PNO, Qty}, {Qty})", 2, 4},
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

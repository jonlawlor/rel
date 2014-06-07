package rel

import (
	"testing"
)

// tests for projection
func TestProject(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	type r1tup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := orders.Project(r1tup{})
	if r1.GoString() != orders.GoString() {
		t.Errorf("orders.Project(PNO, SNO, Qty) = \"%s\", want \"%s\"", r1.GoString(), orders.GoString())
	}
	type r2tup struct {
		PNO int
		SNO int
	}

	r2 := orders.Project(r2tup{})
	r2GoString := `rel.New([]struct {
 PNO int 
 SNO int 
}{
 {1, 1, },
 {1, 2, },
 {1, 3, },
 {1, 4, },
 {1, 5, },
 {1, 6, },
 {2, 1, },
 {2, 2, },
 {3, 2, },
 {4, 2, },
 {4, 4, },
 {4, 5, },
})`
	if GoString(r2) != r2GoString {
		t.Errorf("orders.Project(PNO, SNO) = \"%s\", want \"%s\"", GoString(r2), r2GoString)

	}

	type r3tup struct {
		PNO int
		Qty int
	}
	r3 := orders.Project(r3tup{})
	if Deg(r3) != 2 || Card(r3) != 10 {
		t.Errorf("orders.Project(PNO, Qty) has Deg %d, Card %d, want Deg %d, Card %d", Deg(r3), Card(r3), 2, 10)

	}

	// test the degrees, cardinality, and string representation
	type partsTup struct {
		PNO    int
		PName  string
		Weight float64
		City   string
	}

	rel := parts.Project(partsTup{})
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
	type groupByTup struct {
		City   string
		Weight float64
	}
	type valTup struct {
		Weight float64
	}
	groupFcn := func(val <-chan T) T {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.Weight += v.Weight
		}
		return res
	}
	var relTest = []struct {
		rel          Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{rel, "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City))", 4, 6},
		{rel.Restrict(Attribute("PNO").EQ(1)), "π{PNO, PName, Weight, City}(σ{PNO == 1}(Relation(PNO, PName, Color, Weight, City)))", 4, 1},
		{rel.Project(distinctTup{}), "π{PNO, PName}(Relation(PNO, PName, Color, Weight, City))", 2, 6},
		{rel.Project(nonDistinctTup{}), "π{PName, City}(Relation(PNO, PName, Color, Weight, City))", 2, 6},
		{rel.Rename(titleCaseTup{}), "ρ{Pno, PName, Weight, City}/{PNO, PName, Weight, City}(π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)))", 4, 6},
		{rel.SetDiff(rel.Restrict(Attribute("Weight").LT(15.0))), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) − π{PNO, PName, Weight, City}(σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City)))", 4, 3},
		{rel.Union(rel.Restrict(Attribute("Weight").LE(12.0))), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) ∪ π{PNO, PName, Weight, City}(σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City)))", 4, 6},
		{rel.Join(suppliers, joinTup{}), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(SNO, SName, Status, City)", 6, 10},
		{rel.GroupBy(groupByTup{}, valTup{}, groupFcn), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)).GroupBy({City, Weight}, {Weight})", 2, 3},
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
func BenchmarkProjectTinyIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Tiny relation.
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	r1 := exRel.Project(exTup2{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}

// this doesn't produce much benefit because Project has a short circuit
// for identity Projection.  However, that might get removed once a query
// rewriter is implemented.
func BenchmarkProjectTinyIdentNative(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Tiny relation.
	exRel := exampleRelSlice2(10)

	NativeTups := func(t chan exTup2) {
		go func() {
			for _, tup := range exRel {
				t <- tup
			}
			close(t)
		}()
		return
	}

	NativeProject := func(src chan exTup2, res chan exTup2) {
		go func() {
			for tup := range src {
				res <- exTup2{tup.Foo, tup.Bar}
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
		NativeProject(src, res)
		for _ = range res {
		}
	}
}

func BenchmarkProjectTinyDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Tiny relation where we don't need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		Foo int
	}

	b.ResetTimer()
	r1 := exRel.Project(fooOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}

// this is more indicative of typical project performance
// initial tests show that project incurs a 50% - 100% overhead per attribute
func BenchmarkProjectTinyDistinctNative(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Tiny relation.
	exRel := exampleRelSlice2(10)
	type fooOnly struct {
		Foo int
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

	NativeProject := func(src chan exTup2, res chan fooOnly) {
		go func() {
			for tup := range src {
				res <- fooOnly{tup.Foo}
			}
			close(res)
		}()
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := make(chan exTup2)
		NativeTups(src)
		res := make(chan fooOnly)
		NativeProject(src, res)
		for _ = range res {
		}
	}
}

func BenchmarkProjectTinyNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Tiny relation where we need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	type barOnly struct {
		Bar string
	}

	b.ResetTimer()
	r1 := exRel.Project(barOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}

func BenchmarkProjectSmallIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// small relation.
	exRel := New(exampleRelSlice2(1000), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	r1 := exRel.Project(exTup2{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}
func BenchmarkProjectSmallDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// small relation where we don't need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(1000), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		Foo int
	}

	b.ResetTimer()
	r1 := exRel.Project(fooOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}
func BenchmarkProjectSmallNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// small relation where we need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(1000), [][]string{[]string{"Foo"}})
	type barOnly struct {
		Bar string
	}

	b.ResetTimer()
	r1 := exRel.Project(barOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}

func BenchmarkProjectMediumIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Medium relation.
	exRel := New(exampleRelSlice2(100000), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	r1 := exRel.Project(exTup2{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}
func BenchmarkProjectMediumDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Medium relation where we don't need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(100000), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		Foo int
	}

	b.ResetTimer()
	r1 := exRel.Project(fooOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}
func BenchmarkProjectMediumNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Medium relation where we need to call distinct
	// on the result
	exRel := New(exampleRelSlice2(100000), [][]string{[]string{"Foo"}})
	type barOnly struct {
		Bar string
	}

	b.ResetTimer()
	r1 := exRel.Project(barOnly{})
	for i := 0; i < b.N; i++ {
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
			// do nothing
		}
	}
}

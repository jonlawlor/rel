package rel

import (
	"fmt"

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

	r1 := orders().Project(r1tup{})
	if r1.GoString() != orders().GoString() {
		t.Errorf("orders.Project(PNO, SNO, Qty) = \"%s\", want \"%s\"", r1.GoString(), orders().GoString())
	}
	type r2tup struct {
		PNO int
		SNO int
	}

	r2 := orders().Project(r2tup{})
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
	r3 := orders().Project(r3tup{})
	if Deg(r3) != 2 || Card(r3) != 10 {
		t.Errorf("orders.Project(PNO, Qty) has Deg %d, Card %d, want Deg %d, Card %d", Deg(r3), Card(r3), 2, 10)

	}

	// test the degrees, cardinality, and string representation
	type pTup struct {
		PNO    int
		PName  string
		Weight float64
		City   string
	}
	rel := parts().Project(pTup{})
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
	}
	type groupByTup struct {
		City   string
		Weight float64
	}
	type valTup struct {
		Weight float64
	}
	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Weight += vi.Weight
		}
		return res
	}
	type mapRes struct {
		PNO     int
		PName   string
		Weight2 float64
	}
	mapFcn := func(tup1 pTup) mapRes {
		return mapRes{tup1.PNO, tup1.PName, tup1.Weight * 2}
	}
	mapKeys := [][]string{
		[]string{"PNO"},
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
		{rel.Diff(rel.Restrict(Attribute("Weight").LT(15.0))), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) − π{PNO, PName, Weight, City}(σ{Weight < 15}(Relation(PNO, PName, Color, Weight, City)))", 4, 3},
		{rel.Union(rel.Restrict(Attribute("Weight").LE(12.0))), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) ∪ π{PNO, PName, Weight, City}(σ{Weight <= 12}(Relation(PNO, PName, Color, Weight, City)))", 4, 6},
		{rel.Join(suppliers(), joinTup{}), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)) ⋈ Relation(SNO, SName, Status, City)", 5, 10},
		{rel.GroupBy(groupByTup{}, groupFcn), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)).GroupBy({City, Weight}->{Weight})", 2, 3},
		{rel.Map(mapFcn, mapKeys), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)).Map({PNO, PName, Weight, City}->{PNO, PName, Weight2})", 3, 6},
		{rel.Map(mapFcn, [][]string{}), "π{PNO, PName, Weight, City}(Relation(PNO, PName, Color, Weight, City)).Map({PNO, PName, Weight, City}->{PNO, PName, Weight2})", 3, 6},
	}

	for i, tt := range relTest {
		if err := tt.rel.Err(); err != nil {
			t.Errorf("%d has Err() => %s", i, err.Error())
			continue
		}
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
	res := make(chan pTup)
	cancel := rel.TupleChan(res)
	close(cancel)
	select {
	case <-res:
		t.Errorf("cancel did not end tuple generation")
	default:
		// passed test
	}

	// test errors
	err := fmt.Errorf("testing error")
	rel1 := parts().Project(pTup{}).(*projectExpr)
	rel1.err = err
	rel2 := parts().Project(pTup{}).(*projectExpr)
	rel2.err = err
	res = make(chan pTup)
	_ = rel1.TupleChan(res)
	if _, ok := <-res; ok {
		t.Errorf("did not short circuit TupleChan")
	}
	errTest := []Relation{
		rel1.Rename(titleCaseTup{}),
		rel1.Union(rel2),
		rel.Union(rel2),
		rel1.Diff(rel2),
		rel.Diff(rel2),
		rel1.Join(rel2, orderTup{}),
		rel.Join(rel2, orderTup{}),
		rel1.GroupBy(groupByTup{}, groupFcn),
		rel1.Map(mapFcn, mapKeys),
	}
	for i, errRel := range errTest {
		if errRel.Err() != err {
			t.Errorf("%d did not short circuit error", i)
		}
	}

	errRel := (&errorRel{distinctTup{}, 1, nil}).Project(distinctTup{})
	if c := Card(errRel); c != 1 {
		t.Errorf("errored relation had Card() => %v, wanted %v", c, 1)
	}
}
func BenchmarkProjectTinyIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Tiny relation.
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	r1 := exRel.Project(exTup2{})
	for i := 0; i < b.N; i++ {
		t := make(chan exTup2)
		r1.TupleChan(t)
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
		t := make(chan fooOnly)
		r1.TupleChan(t)
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
		t := make(chan barOnly)
		r1.TupleChan(t)
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
		t := make(chan exTup2)
		r1.TupleChan(t)
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
		t := make(chan fooOnly)
		r1.TupleChan(t)
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
		t := make(chan barOnly)
		r1.TupleChan(t)
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
		t := make(chan exTup2)
		r1.TupleChan(t)
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
		t := make(chan fooOnly)
		r1.TupleChan(t)
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
		t := make(chan barOnly)
		r1.TupleChan(t)
		for _ = range t {
			// do nothing
		}
	}
}

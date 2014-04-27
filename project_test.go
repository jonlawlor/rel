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
	if r2.GoString() != r2GoString {
		t.Errorf("orders.Project(PNO, SNO) = \"%s\", want \"%s\"", r2.GoString(), r2GoString)

	}

	type r3tup struct {
		PNO int
		Qty int
	}
	r3 := orders.Project(r3tup{})
	if r3.Deg() != 2 || r3.Card() != 10 {
		t.Errorf("orders.Project(PNO, Qty) has Deg %d, Card %d, want Deg %d, Card %d", r3.Deg(), r3.Card(), 2, 10)

	}
	return
}

func BenchmarkSimpleProjectTinyIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Tiny relation.
	exRel, _ := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(exTup2{})
	}
}
func BenchmarkSimpleProjectTinyDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Tiny relation where we don't need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		foo int
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(fooOnly{})
	}
}
func BenchmarkSimpleProjectTinyNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Tiny relation where we need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(10), [][]string{[]string{"Foo"}})
	type barOnly struct {
		bar string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(barOnly{})
	}
}

func BenchmarkSimpleProjectSmallIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// small relation.
	exRel, _ := New(exampleRel2(1000), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(exTup2{})
	}
}
func BenchmarkSimpleProjectSmallDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// small relation where we don't need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(1000), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		foo int
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(fooOnly{})
	}
}
func BenchmarkSimpleProjectSmallNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// small relation where we need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(1000), [][]string{[]string{"Foo"}})
	type barOnly struct {
		bar string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(barOnly{})
	}
}

func BenchmarkSimpleProjectMediumIdent(b *testing.B) {
	// test the time it takes to do an identity projection for a
	// Medium relation.
	exRel, _ := New(exampleRel2(100000), [][]string{[]string{"Foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(exTup2{})
	}
}
func BenchmarkSimpleProjectMediumDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Medium relation where we don't need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(100000), [][]string{[]string{"Foo"}})
	type fooOnly struct {
		foo int
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(fooOnly{})
	}
}
func BenchmarkSimpleProjectMediumNonDistinct(b *testing.B) {
	// test the time it takes to do an projection for a
	// Medium relation where we need to call distinct
	// on the result
	exRel, _ := New(exampleRel2(100000), [][]string{[]string{"Foo"}})
	type barOnly struct {
		bar string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exRel.Project(barOnly{})
	}
}

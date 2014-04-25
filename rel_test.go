package rel

import (
	"testing"
)

// test creation of relations, including tests to determine the cost of
// representing slices of structs as relations instead of native.

func BenchmarkSimpleNewTiny(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(10)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}
func BenchmarkNativeNewTiny(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nativeDistinct(exRel)
	}
}

func BenchmarkSimpleNewSmall(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(1000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}
func BenchmarkNativeNewSmall(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nativeDistinct(exRel)
	}
}

func BenchmarkSimpleNewMedium(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(100000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}
func BenchmarkNativeNewMedium(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nativeDistinct(exRel)
	}
}

func BenchmarkSimpleNewLarge(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRel2(10000000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}
func nativeDistinct(tups []exTup2) []exTup2 {
	m := make(map[exTup2]struct{})
	for _, k := range tups {
		m[k] = struct{}{}
	}
	t := make([]exTup2, len(m))
	i := 0
	for k, _ := range m {
		t[i] = k
		i++
	}
	return t
}

type exTup2 struct {
	foo int
	bar string
}

// exampleRel2 creates an example relation with given cardinality
func exampleRel2(c int) (recs []exTup2) {
	for i := 0; i < c; i++ {
		recs = append(recs, exTup2{i, "test"})
	}
	return
}

func TestDeg(t *testing.T) {
	fix := []struct {
		name string
		in   int
		out  int
	}{
		{"suppliers", suppliers.Deg(), 4},
		{"parts", parts.Deg(), 5},
		{"orders", orders.Deg(), 3},
	}
	for i, dt := range fix {
		if dt.in != dt.out {
			t.Errorf("%d. %s.Deg() => %d, want %d", i, dt.name, dt.in, dt.out)
		}
	}
}

func TestCard(t *testing.T) {
	fix := []struct {
		name string
		in   int
		out  int
	}{
		{"suppliers", suppliers.Card(), 5},
		{"parts", parts.Card(), 6},
		{"orders", orders.Card(), 12},
	}
	for i, dt := range fix {
		if dt.in != dt.out {
			t.Errorf("%d. %s.Card() => %d, want %d", i, dt.name, dt.in, dt.out)
		}
	}
}

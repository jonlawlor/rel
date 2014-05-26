package rel

import (
	"testing"
)

// tests & benchmarks for the rel.Slice type

func BenchmarkSliceNewTinySimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelSlice2(10)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewTinyNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelSlice2(10)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewSmallSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelSlice2(1000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewSmallNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelSlice2(1000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewMediumSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelSlice2(100000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewMediumNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelSlice2(100000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewLargeSimple(b *testing.B) {
	// test the time it takes to make a new relation with a given size
	exRel := exampleRelSlice2(10000000)
	ck := [][]string{[]string{"foo"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

func BenchmarkSliceNewLargeNonDistinct(b *testing.B) {
	// test the time it takes to make a new relation with a given size,
	// but without any candidate keys.  The New function will run
	// a distinct on the input data.
	exRel := exampleRelSlice2(10000000)
	ck := [][]string{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(exRel, ck)
	}
}

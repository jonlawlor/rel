package rel

import (
	"testing"
)

// tests & benchmarks for the rel.Chan type

// unlike the rel.Map and rel.Slice type, this has to drain the resulting
// relation, otherwise there will be hanging go-routines.  It would be better
// if we could cancel, but that might require a different type of relation.

// this allows us to drain a channel so that the source goroutines finish
func drain(t chan exTup2) {
	for _ = range t {
	}
	return
}

func BenchmarkSimpleNewChanTiny(b *testing.B) {
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
func BenchmarkNonDistinctNewChanTiny(b *testing.B) {
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

func BenchmarkSimpleNewChanSmall(b *testing.B) {
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
func BenchmarkNonDistinctNewChanSmall(b *testing.B) {
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

func BenchmarkSimpleNewChanMedium(b *testing.B) {
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

func BenchmarkNonDistinctNewChanMedium(b *testing.B) {
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

func BenchmarkSimpleNewChanLarge(b *testing.B) {
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

func BenchmarkNonDistinctNewChanLarge(b *testing.B) {
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

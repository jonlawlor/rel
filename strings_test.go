package rel

import (
	"fmt"
	"testing"
)

// tests for string conversion
// including String, GoString, and benchmarks

func TestGoString(t *testing.T) {
	out := `rel.New([]struct {
 PNO    int     
 PName  string  
 Color  string  
 Weight float64 
 City   string  
}{
 {1, "Nut",   "Red",   12, "London", },
 {2, "Bolt",  "Green", 17, "Paris",  },
 {3, "Screw", "Blue",  17, "Oslo",   },
 {4, "Screw", "Red",   14, "London", },
 {5, "Cam",   "Blue",  12, "Paris",  },
 {6, "Cog",   "Red",   19, "London", },
})`
	if in := fmt.Sprintf("%#v", parts()); in != out {
		t.Errorf("String(Parts) = %q, want %q", in, out)
	}
}

func TestString(t *testing.T) {
	out := ` +------+--------+--------+---------+---------+
 |  PNO |  PName |  Color |  Weight |    City |
 +------+--------+--------+---------+---------+
 |    1 |    Nut |    Red |      12 |  London |
 |    2 |   Bolt |  Green |      17 |   Paris |
 |    3 |  Screw |   Blue |      17 |    Oslo |
 |    4 |  Screw |    Red |      14 |  London |
 |    5 |    Cam |   Blue |      12 |   Paris |
 |    6 |    Cog |    Red |      19 |  London |
 +------+--------+--------+---------+---------+`
	if in := stringTabTable(parts()); in != out {
		t.Errorf("String(Parts) = %v, want %v", in, out)
	}
}

func BenchmarkSimpleStringTiny(b *testing.B) {
	// test the time it takes to turn a relation into a string
	exRel := New(exampleRelSlice2(10), [][]string{[]string{"foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("%v", exRel)
	}
}

func BenchmarkSimpleStringSmall(b *testing.B) {
	// test the time it takes to turn a relation into a string
	exRel := New(exampleRelSlice2(1000), [][]string{[]string{"foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("%v", exRel)
	}
}

func BenchmarkSimpleStringMedium(b *testing.B) {
	// test the time it takes to turn a relation into a string
	exRel := New(exampleRelSlice2(100000), [][]string{[]string{"foo"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("%v", exRel)
	}
}

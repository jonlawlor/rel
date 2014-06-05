package rel

import (
	"testing"
)

// tests for join
func TestJoin(t *testing.T) {

	// TODO(jonlawlor): replace with table driven test?
	type restup struct {
		PNO    int     // from the parts & orders tables
		PName  string  // from the parts table
		Color  string  // from the parts table
		Weight float64 // from the parts table
		City   string  // from the parts table
		SNO    int     // from the orders table
		Qty    int     // from the orders table
	}

	r1 := parts.Join(orders, restup{})
	wantString := `rel.New([]struct {
 PNO    int     
 PName  string  
 Color  string  
 Weight float64 
 City   string  
 SNO    int     
 Qty    int     
}{
 {1, "Nut",   "Red",   12, "London", 1, 300, },
 {1, "Nut",   "Red",   12, "London", 2, 200, },
 {1, "Nut",   "Red",   12, "London", 3, 400, },
 {1, "Nut",   "Red",   12, "London", 4, 200, },
 {1, "Nut",   "Red",   12, "London", 5, 100, },
 {1, "Nut",   "Red",   12, "London", 6, 100, },
 {2, "Bolt",  "Green", 17, "Paris",  1, 300, },
 {2, "Bolt",  "Green", 17, "Paris",  2, 400, },
 {3, "Screw", "Blue",  17, "Oslo",   2, 200, },
 {4, "Screw", "Red",   14, "London", 2, 200, },
 {4, "Screw", "Red",   14, "London", 4, 300, },
 {4, "Screw", "Red",   14, "London", 5, 400, },
})`

	if GoString(r1) != wantString {
		t.Errorf("Join(parts, orders, restup{}).GoString() = \"%s\", want \"%s\"", GoString(r1), wantString)
	}
	return
}

func BenchmarkJoin(b *testing.B) {
	type restup struct {
		PNO    int     // from the parts & orders tables
		PName  string  // from the parts table
		Color  string  // from the parts table
		Weight float64 // from the parts table
		City   string  // from the parts table
		SNO    int     // from the orders table
		Qty    int     // from the orders table
	}

	r1 := parts.Join(orders, restup{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// each iteration produces 12 tuples
		t := make(chan T)
		r1.Tuples(t)
		for _ = range t {
		}
	}
}

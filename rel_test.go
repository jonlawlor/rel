package rel

import (
	"fmt"
	"testing"
)

// data for a Suppliers, Parts & orders database, using the example provided
// by C. J. Date in his book "Database in Depth" in Figure 1-3.
// I think he might have a different type for the SNO and PNO columns, but int
// probably works just as well.  We might want to define a type alias for it.

// Suppliers relation, with candidate keys {SNO}, {SName}
var Suppliers = New([]struct {
	SNO    int
	SName  string
	Status int
	City   string
}{
	{1, "Smith", 20, "London"},
	{2, "Jones", 10, "Paris"},
	{3, "Blake", 30, "Paris"},
	{4, "Clark", 20, "London"},
	{5, "Adams", 30, "Athens"},
})

// Parts relation, with candidate keys {PNO}
var Parts = New([]struct {
	PNO    int
	PName  string
	Color  string
	Weight float64
	City   string
}{
	{1, "Nut", "Red", 12.0, "London"},
	{2, "Bolt", "Green", 17.0, "Paris"},
	{3, "Screw", "Blue", 17.0, "Oslo"},
	{4, "Screw", "Red", 14.0, "London"},
	{5, "Cam", "Blue", 12.0, "Paris"},
	{6, "Cog", "Red", 19.0, "London"},
})

// Orders relation, with candidate keys {PNO, SNO}
var Orders = New([]struct {
	PNO int
	SNO int
	Qty int
}{
	{1, 1, 300},
	{1, 2, 200},
	{1, 3, 400},
	{1, 4, 200},
	{1, 5, 100},
	{1, 6, 100},
	{2, 1, 300},
	{2, 2, 400},
	{3, 2, 200},
	{4, 2, 200},
	{4, 4, 300},
	{4, 5, 400},
})

func TestString(t *testing.T) {
	// TODO(jonlawlor): replace with table driven test?
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
	if in := fmt.Sprintf("%v", Parts); in != out {
		t.Errorf("String(Parts) = \"%s\", want \"%s\"", in, out)
	}
}

func TestDeg(t *testing.T) {
	fix := []struct {
		name string
		in   int
		out  int
	}{
		{"Suppliers", Suppliers.Deg(), 4},
		{"Parts", Parts.Deg(), 5},
		{"Orders", Orders.Deg(), 3},
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
		{"Suppliers", Suppliers.Card(), 5},
		{"Parts", Parts.Card(), 6},
		{"Orders", Orders.Card(), 12},
	}
	for i, dt := range fix {
		if dt.in != dt.out {
			t.Errorf("%d. %s.Card() => %d, want %d", i, dt.name, dt.in, dt.out)
		}
	}
}

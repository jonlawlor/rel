package rel

// This file contains example data for a suppliers, parts & orders database, using
// the example provided by C. J. Date in his book "Database in Depth" in Figure 1-3.
// I think he might have a different type for the SNO and PNO columns, but int
// probably works just as well.  We might want to define a type alias for it.

// Maybe we should make this into an example function instead of top level
// variables?

// suppliers relation, with candidate keys {SNO}
// the {SName} key is also possible to use
var suppliers, _ = New([]struct {
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
}, [][]string{
	[]string{"SNO"},
})

// parts relation, with candidate keys {PNO}
var parts, _ = New([]struct {
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
}, [][]string{
	[]string{"PNO"},
})

// orders relation, with candidate keys {PNO, SNO}
var orders, _ = New([]struct {
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
}, [][]string{
	[]string{"PNO", "SNO"},
})

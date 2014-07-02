// a set of examples for the rel package

package rel_test

import (
	"fmt"
	"github.com/jonlawlor/rel"
	"sort"
)

type supplierTup struct {
	SNO    int
	SName  string
	Status int
	City   string
}

type suppliers []supplierTup

func (tups suppliers) Len() int           { return len(tups) }
func (tups suppliers) Swap(i, j int)      { tups[i], tups[j] = tups[j], tups[i] }
func (tups suppliers) Less(i, j int) bool { return tups[i].SNO < tups[j].SNO }

func ExampleUnion() {
	// the type of the tuples in the relation
	// defined elsewhere...
	// type supplierTup struct {
	// 	SNO    int
	// 	SName  string
	// 	Status int
	// 	City   string
	// }
	r1 := rel.New([]supplierTup{
		{1, "Smith", 20, "London"},
		{2, "Jones", 10, "Paris"},
		{3, "Blake", 30, "Paris"},
	}, [][]string{
		[]string{"SNO"}, // the candidat key
	})
	r2 := rel.New([]supplierTup{
		{4, "Clark", 20, "London"},
		{5, "Adams", 30, "Athens"},
		{6, "Coppola Ristorante", 1, "New Providence"},
	}, [][]string{
		[]string{"SNO"}, // the candidat key
	})
	r3 := r1.Union(r2)

	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := suppliers{}
	t := make(chan supplierTup)
	r3.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}

	// defined elsewhere...
	// type suppliers []supplierTup
	//
	// func (tups suppliers) Len() int { return len(tups) }
	// func (tups suppliers) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups suppliers) Less(i, j int) bool { return tups[i].SNO < tups[j].SNO }

	sort.Sort(res)
	r4 := rel.New(res, [][]string{
		[]string{"SNO"}, // the candidate key
	})

	fmt.Printf("%v\n", r3)
	fmt.Println(rel.PrettyPrint(r4))
	// Output:
	// Relation(SNO, SName, Status, City) ∪ Relation(SNO, SName, Status, City)
	//  +------+---------------------+---------+-----------------+
	//  |  SNO |               SName |  Status |            City |
	//  +------+---------------------+---------+-----------------+
	//  |    1 |               Smith |      20 |          London |
	//  |    2 |               Jones |      10 |           Paris |
	//  |    3 |               Blake |      30 |           Paris |
	//  |    4 |               Clark |      20 |          London |
	//  |    5 |               Adams |      30 |          Athens |
	//  |    6 |  Coppola Ristorante |       1 |  New Providence |
	//  +------+---------------------+---------+-----------------+

}

func ExampleDiff() {
	// the type of the tuples in the relation
	// defined elsewhere
	// type supplierTup struct {
	//	SNO    int
	//	SName  string
	//	Status int
	//	City   string
	// }
	r1 := rel.New([]supplierTup{
		{1, "Smith", 20, "London"},
		{2, "Jones", 10, "Paris"},
		{3, "Blake", 30, "Paris"},
		{4, "Clark", 20, "London"},
		{5, "Adams", 30, "Athens"},
		{6, "Coppola Ristorante", 1, "New Providence"},
	}, [][]string{
		[]string{"SNO"}, // the candidat key
	})
	r2 := rel.New([]supplierTup{
		{1, "Smith", 20, "London"},
		{2, "Jones", 10, "Paris"},
		{3, "Blake", 30, "Paris"},
		{4, "Clark", 20, "London"},
		{5, "Adams", 30, "Athens"},
	}, [][]string{
		[]string{"SNO"}, // the candidat key
	})

	r3 := r1.Diff(r2)
	fmt.Println(r3)
	fmt.Println(rel.PrettyPrint(r3))
	// in this case there is a single tuple so no ordering is needed
	// Output:
	// Relation(SNO, SName, Status, City) ∪ Relation(SNO, SName, Status, City)
	//  +------+---------------------+---------+-----------------+
	//  |  SNO |               SName |  Status |            City |
	//  +------+---------------------+---------+-----------------+
	//  |    6 |  Coppola Ristorante |       1 |  New Providence |
	//  +------+---------------------+---------+-----------------+
}

type PNOSNO struct {
	PNO int
	SNO int
}

type PNOSNOs []PNOSNO

func (tups PNOSNOs) Len() int      { return len(tups) }
func (tups PNOSNOs) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
func (tups PNOSNOs) Less(i, j int) bool {
	return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO)
}

func ExampleProject_Distinct() {
	// the type of the tuples in the input relation

	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := rel.New([]orderTup{
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

	// the type of the tuples in the output relation
	// it is distinct because it contains attributes that are a subset of
	// one of the candidate keys.
	// defined elsewhere:
	// type PNOSNO struct {
	//	PNO int
	// 	SNO int
	// }
	r2 := r1.Project(PNOSNO{})

	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := PNOSNOs{}
	t := make(chan PNOSNO)
	r2.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}

	// defined elsewhere...
	// type PNOSNOs []PNOSNO
	//
	// func (tups PNOSNOs) Len() int           { return len(tups) }
	// func (tups PNOSNOs) Swap(i, j int)      { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups PNOSNOs) Less(i, j int) bool { return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO) }

	sort.Sort(res)
	r3 := rel.New(res, [][]string{
		[]string{"SNO", "PNO"}, // the candidate key
	})

	fmt.Printf("%v\n", r2)
	fmt.Println(rel.PrettyPrint(r3))
	// Output:
	// π{PNO, SNO}(Relation(PNO, SNO, Qty))
	//  +------+------+
	//  |  PNO |  SNO |
	//  +------+------+
	//  |    1 |    1 |
	//  |    1 |    2 |
	//  |    1 |    3 |
	//  |    1 |    4 |
	//  |    1 |    5 |
	//  |    1 |    6 |
	//  |    2 |    1 |
	//  |    2 |    2 |
	//  |    3 |    2 |
	//  |    4 |    2 |
	//  |    4 |    4 |
	//  |    4 |    5 |
	//  +------+------+

}

type PNOQty struct {
	PNO int
	Qty int
}

type PNOQtys []PNOQty

func (tups PNOQtys) Len() int      { return len(tups) }
func (tups PNOQtys) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
func (tups PNOQtys) Less(i, j int) bool {
	if tups[i].PNO < tups[j].PNO {
		return true
	} else if tups[i].PNO == tups[j].PNO && tups[i].Qty < tups[j].Qty {
		return true
	}
	return false
}

func ExampleProject_NonDistinct() {
	// the type of the tuples in the input relation

	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := rel.New([]orderTup{
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

	// the type of the tuples in the output relation
	// it is not distinct because it does not contain attributes that are a
	// subset of one of the candidate keys.
	// defined elsewhere...
	// type PNOQty struct {
	//	PNO int
	//	Qty int
	//}
	r2 := r1.Project(PNOQty{})

	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := PNOQtys{}
	t := make(chan PNOQty)
	r2.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}
	sort.Sort(res)

	r3 := rel.New(res, [][]string{
		[]string{"PNO", "Qty"}, // the candidate key
	})

	fmt.Printf("%v\n", r2)
	fmt.Println(rel.PrettyPrint(r3))

	// defined elsewhere...
	// type PNOQtys []PNOQty
	//
	// func (tups PNOQtys) Len() int           { return len(tups) }
	// func (tups PNOQtys) Swap(i, j int)      { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups PNOQtys) Less(i, j int) bool {
	// 	if tups[i].PNO < tups[j].PNO {
	// 		return true
	// 	} else if tups[i].PNO == tups[j].PNO && tups[i].Qty < tups[j].Qty {
	// 		return true
	// 	}
	// 	return false
	// }

	// Output:
	// π{PNO, Qty}(Relation(PNO, SNO, Qty))
	//  +------+------+
	//  |  PNO |  Qty |
	//  +------+------+
	//  |    1 |  100 |
	//  |    1 |  200 |
	//  |    1 |  300 |
	//  |    1 |  400 |
	//  |    2 |  300 |
	//  |    2 |  400 |
	//  |    3 |  200 |
	//  |    4 |  200 |
	//  |    4 |  300 |
	//  |    4 |  400 |
	//  +------+------+
}

func ExampleRename() {
	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := rel.New([]orderTup{
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

	// the type of the tuples in the output relation
	// in this case the position of the fields is significant.  They correspond
	// to the fields in orderTup.
	type titleCaseTup struct {
		Pno int
		Sno int
		Qty int
	}

	r2 := r1.Rename(titleCaseTup{})
	fmt.Println(r2)
	fmt.Println(rel.PrettyPrint(r2))
	// currenty rename does not result in a non deterministic ordering

	// Output:
	// ρ{Pno, Sno, Qty}/{PNO, SNO, Qty}(Relation(PNO, SNO, Qty))
	//  +------+------+------+
	//  |  Pno |  Sno |  Qty |
	//  +------+------+------+
	//  |    1 |    1 |  300 |
	//  |    1 |    2 |  200 |
	//  |    1 |    3 |  400 |
	//  |    1 |    4 |  200 |
	//  |    1 |    5 |  100 |
	//  |    1 |    6 |  100 |
	//  |    2 |    1 |  300 |
	//  |    2 |    2 |  400 |
	//  |    3 |    2 |  200 |
	//  |    4 |    2 |  200 |
	//  |    4 |    4 |  300 |
	//  |    4 |    5 |  400 |
	//  +------+------+------+

}

func ExampleRestrict() {
	// the type of the tuples in the relation
	type supplierTup struct {
		SNO    int
		SName  string
		Rating int
		City   string
	}
	r1 := rel.New([]supplierTup{
		{1, "Smith", 3, "London"},
		{2, "Jones", 1, "Paris"},
		{3, "Blake", 3, "Paris"},
		{4, "Clark", 2, "London"},
		{5, "Adams", 3, "Athens"},
		{6, "Coppola Ristorante", 5, "New Providence"},
	}, [][]string{
		[]string{"SNO"}, // the candidat key
	})

	// chose records with rating greater than 4
	r2 := r1.Restrict(rel.Attribute("Rating").GT(4))
	fmt.Println(r2)
	fmt.Println(rel.PrettyPrint(r2))

	// Output:
	// σ{Rating > 4}(Relation(SNO, SName, Rating, City))
	//  +------+---------------------+---------+-----------------+
	//  |  SNO |               SName |  Rating |            City |
	//  +------+---------------------+---------+-----------------+
	//  |    6 |  Coppola Ristorante |       5 |  New Providence |
	//  +------+---------------------+---------+-----------------+

}

type joinTup struct {
	PNO    int
	SNO    int
	Qty    int
	SName  string
	Status int
	City   string
}

type joinTups []joinTup

func (tups joinTups) Len() int      { return len(tups) }
func (tups joinTups) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
func (tups joinTups) Less(i, j int) bool {
	return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO)
}

func ExampleJoin() {
	// suppliers relation, with candidate keys {SNO}
	// the {SName} key is also possible to use
	// type supplierTup struct {
	// 	SNO    int
	// 	SName  string
	// 	Status int
	// 	City   string
	// }

	suppliers := rel.New([]supplierTup{
		{1, "Smith", 20, "London"},
		{2, "Jones", 10, "Paris"},
		{3, "Blake", 30, "Paris"},
		{4, "Clark", 20, "London"},
		{5, "Adams", 30, "Athens"},
	}, [][]string{
		[]string{"SNO"},
	})

	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	orders := rel.New([]orderTup{
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

	// the type of the resulting tuples
	// defined elsewhere...
	// type joinTup struct {
	// 	PNO    int
	// 	SNO    int
	// 	Qty    int
	// 	SName  string
	// 	Status int
	// 	City   string
	// }

	partsSuppliers := orders.Join(suppliers, joinTup{})

	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := joinTups{}
	t := make(chan joinTup)
	partsSuppliers.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}
	sort.Sort(res)

	partsSuppliersOrdered := rel.New(res, [][]string{
		[]string{"PNO", "SNO"}, // the candidate key
	})

	fmt.Printf("%v\n", partsSuppliers)
	fmt.Println(rel.PrettyPrint(partsSuppliersOrdered))

	// defined elsewhere...
	// type joinTups []joinTup
	//
	// func (tups joinTups) Len() int      { return len(tups) }
	// func (tups joinTups) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups joinTups) Less(i, j int) bool {
	// 	return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO)
	// }

	// Output:
	// Relation(PNO, SNO, Qty) ⋈ Relation(SNO, SName, Status, City)
	//  +------+------+------+--------+---------+---------+
	//  |  PNO |  SNO |  Qty |  SName |  Status |    City |
	//  +------+------+------+--------+---------+---------+
	//  |    1 |    1 |  300 |  Smith |      20 |  London |
	//  |    1 |    2 |  200 |  Jones |      10 |   Paris |
	//  |    1 |    3 |  400 |  Blake |      30 |   Paris |
	//  |    1 |    4 |  200 |  Clark |      20 |  London |
	//  |    1 |    5 |  100 |  Adams |      30 |  Athens |
	//  |    2 |    1 |  300 |  Smith |      20 |  London |
	//  |    2 |    2 |  400 |  Jones |      10 |   Paris |
	//  |    3 |    2 |  200 |  Jones |      10 |   Paris |
	//  |    4 |    2 |  200 |  Jones |      10 |   Paris |
	//  |    4 |    4 |  300 |  Clark |      20 |  London |
	//  |    4 |    5 |  400 |  Adams |      30 |  Athens |
	//  +------+------+------+--------+---------+---------+
}

type PNO struct {
	PNO int
	Qty int
}

type PNOs []PNO

func (tups PNOs) Len() int           { return len(tups) }
func (tups PNOs) Swap(i, j int)      { tups[i], tups[j] = tups[j], tups[i] }
func (tups PNOs) Less(i, j int) bool { return tups[i].PNO < tups[j].PNO }

func ExampleGroupBy() {

	// the type of the tuples in the input relation
	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := rel.New([]orderTup{
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

	// this is the type of the resulting tuples.  Because PNO is not a part of
	// the RETURN of the groupFcn, it is used to determine the unique groups of
	// the resulting relation.
	// defined elsewhere...
	// type PNO struct {
	// 	PNO int
	// 	Qty int
	// }

	// this is (in this case) both the type of the tuples that get accumulated,
	// and also the resulting type of the accumulation.
	type valTup struct {
		Qty int
	}

	// a function which sums the quantities of orders
	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Qty += vi.Qty
		}
		return res
	}

	r2 := r1.GroupBy(PNO{}, groupFcn)
	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := PNOs{}
	t := make(chan PNO)
	r2.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}
	sort.Sort(res)

	r3 := rel.New(res, [][]string{
		[]string{"PNO"}, // the candidate key
	})

	fmt.Printf("%v\n", r2)
	fmt.Println(rel.PrettyPrint(r3))

	// defined elsewhere...
	// type PNOs []PNO
	//
	// func (tups PNOs) Len() int      { return len(tups) }
	// func (tups PNOs) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups PNOs) Less(i, j int) bool { return tups[i].PNO < tups[j].PNO }

	// Output:
	// Relation(PNO, SNO, Qty).GroupBy({PNO, Qty}->{Qty})
	//  +------+-------+
	//  |  PNO |   Qty |
	//  +------+-------+
	//  |    1 |  1300 |
	//  |    2 |   700 |
	//  |    3 |   200 |
	//  |    4 |   900 |
	//  +------+-------+
}

type qtyDouble struct {
	PNO  int
	SNO  int
	Qty1 int
	Qty2 int
}

type qtyDoubles []qtyDouble

func (tups qtyDoubles) Len() int      { return len(tups) }
func (tups qtyDoubles) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
func (tups qtyDoubles) Less(i, j int) bool {
	return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO)
}

func ExampleMap() {
	type orderTup struct {
		PNO int
		SNO int
		Qty int
	}

	r1 := rel.New([]orderTup{
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

	// defined elsewhere...
	// type qtyDouble struct {
	//	PNO  int
	//	SNO  int
	//	Qty1 int
	//	Qty2 int
	//}
	mapFcn := func(tup1 orderTup) qtyDouble {
		return qtyDouble{tup1.PNO, tup1.SNO, tup1.Qty, tup1.Qty * 2}
	}

	// an arbitrary function could modify any of the columns, which means
	// we need to explain what the new Keys (if any) will be afterwards
	mapKeys := [][]string{
		[]string{"PNO", "SNO"},
	}

	r2 := r1.Map(mapFcn, mapKeys)
	// order the output and stick it back into a slice
	// this is really just to get the output into a consistent order.  If you
	// don't care about the order, you don't have to do this.  Currently
	// ordering is not part of the rel package (it isn't a part of relational
	// algebra!)
	res := qtyDoubles{}
	t := make(chan qtyDouble)
	r2.TupleChan(t)
	for v := range t {
		res = append(res, v)
	}
	sort.Sort(res)

	r3 := rel.New(res, [][]string{
		[]string{"PNO", "SNO"}, // the candidate key
	})

	fmt.Printf("%v\n", r2)
	fmt.Println(rel.PrettyPrint(r3))

	// defined elsewhere...
	// type qtyDoubles []qtyDouble
	//
	// func (tups qtyDoubles) Len() int      { return len(tups) }
	// func (tups qtyDoubles) Swap(i, j int) { tups[i], tups[j] = tups[j], tups[i] }
	// func (tups qtyDoubles) Less(i, j int) bool {
	// 	return tups[i].PNO < tups[j].PNO || (tups[i].PNO == tups[j].PNO && tups[i].SNO < tups[j].SNO)
	//}

	// Output:
	// Relation(PNO, SNO, Qty).Map({PNO, SNO, Qty}->{PNO, SNO, Qty1, Qty2})
	//  +------+------+-------+-------+
	//  |  PNO |  SNO |  Qty1 |  Qty2 |
	//  +------+------+-------+-------+
	//  |    1 |    1 |   300 |   600 |
	//  |    1 |    2 |   200 |   400 |
	//  |    1 |    3 |   400 |   800 |
	//  |    1 |    4 |   200 |   400 |
	//  |    1 |    5 |   100 |   200 |
	//  |    1 |    6 |   100 |   200 |
	//  |    2 |    1 |   300 |   600 |
	//  |    2 |    2 |   400 |   800 |
	//  |    3 |    2 |   200 |   400 |
	//  |    4 |    2 |   200 |   400 |
	//  |    4 |    4 |   300 |   600 |
	//  |    4 |    5 |   400 |   800 |
	//  +------+------+-------+-------+

}

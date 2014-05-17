// join implements a natural join expression in relational algebra

package rel

type JoinExpr struct {
	source1 Relation
	source2 Relation
}

/*  needs to be rewritten
func (r1 Simple) JoinExpr(r2 Relation, t3 interface{}) (r3 Relation) {

	mc := MaxConcurrent
	e3 := reflect.TypeOf(t3)

	// create indexes between the three headings
	h1 := r1.Heading()
	h2 := r2.Heading()
	h3 := interfaceHeading(t3)

	map12 := attributeMap(h1, h2) // used to determine equality
	map31 := attributeMap(h3, h1) // used to construct returned values
	map32 := attributeMap(h3, h2) // used to construct returned values

	// create a channel over the body
	tups := make(chan interface{})
	r2.Tuples(tups)

	// channel of the output tuples
	res := make(chan interface{})

	// done is used to signal when each of the worker goroutines
	// finishes processing the join operation
	done := make(chan struct{})
	go func() {
		for i := 0; i < mc; i++ {
			<-done
		}
		close(res)
	}()

	// create a go routine that generates the join for each of the input tuples
	for i := 0; i < mc; i++ {
		go func() {
			for tup2 := range tups {
				rtup2 := reflect.ValueOf(tup2)
				for j := 0; j < r1.Card(); j++ {
					if partialEquals(reflect.ValueOf(r1.Body[j]), rtup2, map12) {
						tup3 := reflect.Indirect(reflect.New(e3))
						combineTuples2(&tup3, reflect.ValueOf(r1.Body[j]), map31)
						combineTuples2(&tup3, rtup2, map32)
						res <- tup3.Interface()
					}
				}
			}
			done <- struct{}{}
		}()
	}

	// create a new body with the results and accumulate them
	b := make([]interface{}, 0)
	for tup := range res {
		b = append(b, tup)
	}

	// determine the new candidate keys

	return
}
*/

//groupby implements a group by expression in relational algebra

package rel

type GroupByExpr struct {
	source Relation

	// zero is the resulting relation tuple type
	zero T

	// valZero is the tuple type of the values provided to the grouping
	// function.  We might want to infer it from the grouping function
	// instead though, like restrict does?
	valZero T

	// gfcn is the function which when given a channel of tuples, returns
	// the value of the group after the input channel is closed.
	// We might want to be able to short circuit this evaluation in a few
	// cases though?
	gfcn func(chan interface{}) interface{}
}

/* need rewrite:
// the implementation for groupby creates a map from the groups to
// a set of channels, and then creates those channels as new groups
// are discovered.  Those channels each have a goroutine that concurrently
// consumes the channel results (although that might simply be an
// accumulation if the aggregate can't be performed on partial results)
// and then when all of the values in the intial relation are done, every
// group chan is closed, which should allow the group go routines to
// complete their work, and then send a done signal to a channel which
// can then close the result channel.

func (r1 Simple) GroupByExpr(t2 interface{}, vt interface{}, gfcn func(chan interface{}) interface{}) (r2 Relation) {
	// figure out the new elements used for each of the derived types
	e2 := reflect.TypeOf(t2) // type of the resulting relation's tuples
	ev := reflect.TypeOf(vt) // type of the tuples put into groupby values

	// note: if for some reason this is called on a grouping that includes
	// a candidate key, then this function should instead act as a map, and
	// we might want to have a different codepath for that.

	// create the map for channels to grouping goroutines
	groupMap := make(map[interface{}]chan interface{})

	// create waitgroup that indicates that the computations are complete
	var wg sync.WaitGroup

	// create the channel of tuples from r1
	tups := make(chan interface{})
	r1.Tuples(tups)

	// results come back through the res channel
	res := make(chan interface{})

	// for each of the tuples, extract the group values out and set
	// the ones that are not in vtup to the values in the tuple.
	// then, if the tuple does not exist in the groupMap, create a
	// new channel and launch a new goroutine to consume the channel,
	// increment the waitgroup, and finally send the vtup to the
	// channel.

	// figure out where in each of the structs the group and value
	// attributes are found
	e2fieldMap := fieldMap(r1.tupleType, e2)
	evfieldMap := fieldMap(r1.tupleType, ev)

	// map from the values to the group (with zeros in the value fields)
	// I couldn't figure out a way to assign the values into the group
	// by modifying it using reflection though so we end up allocating a
	// new element.
	// TODO(jonlawlor): figure out how to avoid reallocation
	vgfieldMap := fieldMap(e2, ev)

	// determine the new candidate keys, which can be any of the original
	// candidate keys that are a subset of the group (which would also
	// mean that every tuple in the original relation is in its own group
	// in the resulting relation, which means the groupby function was
	// just a map) or the group itself.

	// make a new map with values from e2fieldMap that are not in
	// evfieldmap (do we have enough maps yet???)
	groupFieldMap := make(map[string]fieldIndex)
	for name, v := range e2fieldMap {
		if _, isValue := evfieldMap[name]; !isValue {
			groupFieldMap[name] = v
		}
	}
	ck2 := subsetCandidateKeys(r1.CKeys, r1.Names, groupFieldMap)

	for tup := range tups {
		// this reflection may be a bottleneck, and we may be able to
		// replace it with a concurrent version.
		gtupi, vtupi := partialProject(reflect.ValueOf(tup), e2, ev, e2fieldMap, evfieldMap)

		// the map cannot be accessed concurrently though
		// a lock needs to be placed here
		if _, exists := groupMap[gtupi]; !exists {
			wg.Add(1)
			// create the channel
			groupChan := make(chan interface{})
			groupMap[gtupi] = groupChan
			// remove the lock

			// launch a goroutine which consumes values from the group,
			// applies the grouping function, and then when all values
			// are sent, gets the result from the grouping function and
			// puts it into the result tuple, which it then returns
			go func(gtupi interface{}, groupChan chan interface{}) {
				defer wg.Done()
				// run the grouping function and turn the result
				// into the reflect.Value
				vtup := reflect.ValueOf(gfcn(groupChan))
				// combine the returned values with the group tuple
				// to create the new complete tuple
				res <- combineTuples(reflect.ValueOf(gtupi), vtup, e2, vgfieldMap).Interface()
			}(gtupi, groupChan)
		}
		// this send can also be done concurrently, or we could buffer
		// the channel
		groupMap[gtupi] <- vtupi
	}

	// close all of the group channels so the processes can finish
	// this can only be done after the tuples in the original relation
	// have all been sent to the groups
	for _, v := range groupMap {
		close(v)
	}

	// start a process to close the results channel when the waitgroup
	// is finished
	go func() {
		wg.Wait()
		close(res)
	}()

	// determine the new names and types
	cn, ct := namesAndTypes(e2)

	if len(ck2) == 0 {
		ck2 = append(ck2, cn)
	}
	// accumulate the results into a new relation
	b := make([]interface{}, 0)
	for tup := range res {
		b = append(b, tup)
	}
	return Simple{cn, ct, b, ck2, e2}
}
*/

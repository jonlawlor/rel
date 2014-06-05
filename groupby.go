//groupby implements a group by expression in relational algebra

package rel

import (
	"reflect"
	"sync"
)

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
	gfcn func(chan T) T
}

// the implementation for groupby creates a map from the groups to
// a set of channels, and then creates those channels as new groups
// are discovered.  Those channels each have a goroutine that concurrently
// consumes the channel results (although that might simply be an
// accumulation if the aggregate can't be performed on partial results)
// and then when all of the values in the intial relation are done, every
// group chan is closed, which should allow the group go routines to
// complete their work, and then send a done signal to a channel which
// can then close the result channel.

func (r *GroupByExpr) Tuples(t chan T) {
	// figure out the new elements used for each of the derived types
	e1 := reflect.TypeOf(r.source.Zero())
	e2 := reflect.TypeOf(r.zero)    // type of the resulting relation's tuples
	ev := reflect.TypeOf(r.valZero) // type of the tuples put into groupby values

	// note: if for some reason this is called on a grouping that includes
	// a candidate key, then this function should instead act as a map, and
	// we might want to have a different codepath for that.

	// create the map for channels to grouping goroutines
	groupMap := make(map[T]chan T)

	// create waitgroup that indicates that the computations are complete
	var wg sync.WaitGroup

	// create the channel of tuples from source
	body := make(chan T)
	r.source.Tuples(body)

	// for each of the tuples, extract the group values out and set
	// the ones that are not in vtup to the values in the tuple.
	// then, if the tuple does not exist in the groupMap, create a
	// new channel and launch a new goroutine to consume the channel,
	// increment the waitgroup, and finally send the vtup to the
	// channel.

	// figure out where in each of the structs the group and value
	// attributes are found
	e2fieldMap := fieldMap(e1, e2)
	evfieldMap := fieldMap(e1, ev)

	// map from the values to the group (with zeros in the value fields)
	// I couldn't figure out a way to assign the values into the group
	// by modifying it using reflection though so we end up allocating a
	// new element.
	// TODO(jonlawlor): figure out how to avoid reallocation
	vgfieldMap := fieldMap(e2, ev)

	go func(b1, res chan T) {
		for tup := range b1 {
			// this reflection may be a bottleneck, and we may be able to
			// replace it with a concurrent version.
			gtupi, vtupi := partialProject(reflect.ValueOf(tup), e2, ev, e2fieldMap, evfieldMap)

			// the map cannot be accessed concurrently though
			// a lock needs to be placed here
			if _, exists := groupMap[gtupi]; !exists {
				wg.Add(1)
				// create the channel
				groupChan := make(chan T)
				groupMap[gtupi] = groupChan
				// remove the lock

				// launch a goroutine which consumes values from the group,
				// applies the grouping function, and then when all values
				// are sent, gets the result from the grouping function and
				// puts it into the result tuple, which it then returns
				go func(gtupi T, groupChan chan T) {
					defer wg.Done()
					// run the grouping function and turn the result
					// into the reflect.Value
					vtup := reflect.ValueOf(r.gfcn(groupChan))
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
	}(body, t)

	// start a process to close the results channel when the waitgroup
	// is finished
	go func(res chan T) {
		wg.Wait()
		close(res)
	}(t)

	return
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *GroupByExpr) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *GroupByExpr) CKeys() CandKeys {
	// determine the new candidate keys, which can be any of the original
	// candidate keys that are a subset of the group (which would also
	// mean that every tuple in the original relation is in its own group
	// in the resulting relation, which means the groupby function was
	// just a map) or the group itself.

	e1 := reflect.TypeOf(r.source.Zero())
	e2 := reflect.TypeOf(r.zero)    // type of the resulting relation's tuples
	ev := reflect.TypeOf(r.valZero) // type of the tuples put into groupby values

	// note: if for some reason this is called on a grouping that includes
	// a candidate key, then this function should instead act as a map, and
	// we might want to have a different codepath for that.

	// for each of the tuples, extract the group values out and set
	// the ones that are not in vtup to the values in the tuple.
	// then, if the tuple does not exist in the groupMap, create a
	// new channel and launch a new goroutine to consume the channel,
	// increment the waitgroup, and finally send the vtup to the
	// channel.

	// figure out where in each of the structs the group and value
	// attributes are found
	e2fieldMap := fieldMap(e1, e2)
	evfieldMap := fieldMap(e1, ev)

	groupFieldMap := make(map[Attribute]fieldIndex)
	for name, v := range e2fieldMap {
		if _, isValue := evfieldMap[name]; !isValue {
			groupFieldMap[name] = v
		}
	}
	names := fieldNames(e2)

	ck2 := subsetCandidateKeys(r.CKeys(), names, groupFieldMap)

	// determine the new names and types
	cn := fieldNames(e2)

	if len(ck2) == 0 {
		ck2 = append(ck2, cn)
	}

	return ck2
}

// GoString returns a text representation of the Relation
func (r *GroupByExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *GroupByExpr) String() string {
	return stringTabTable(r)
}

// rewrite is more difficult in groupby, because it is not a part of
// relational algebra.  The current implementation only avoids no ops.

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *GroupByExpr) Project(z2 T) Relation {
	att2 := fieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &ProjectExpr{r1, z2}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r *GroupByExpr) Restrict(p Predicate) Relation {
	return &RestrictExpr{r, p}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *GroupByExpr) Rename(z2 T) Relation {
	return &RenameExpr{r1, z2}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *GroupByExpr) Union(r2 Relation) Relation {
	return &UnionExpr{r1, r2}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *GroupByExpr) SetDiff(r2 Relation) Relation {
	return &SetDiffExpr{r1, r2}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *GroupByExpr) Join(r2 Relation, zero T) Relation {
	return &JoinExpr{r1, r2, zero}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *GroupByExpr) GroupBy(t2, vt T, gfcn func(chan T) T) Relation {
	return &GroupByExpr{r1, t2, vt, gfcn}
}

//groupby implements a group by expression in relational algebra

package rel

import (
	"reflect"
	"strings"
	"sync"
)

// groupByExpr represents a group by expression
type groupByExpr struct {
	source1 Relation

	// zero is the resulting relation tuple type
	zero interface{}

	// valType is the tuple type of the values provided to the grouping
	// function by the input chan
	valType reflect.Type

	// resType is the tuple type of the values returned from the grouping
	// function.
	resType reflect.Type

	// gfcn is the function which when given a channel of tuples, returns
	// the value of the group after the input channel is closed.
	gfcn reflect.Value

	// err has the first error encountered during construction or evaluation
	err error
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

// TupleChan sends each tuple in the relation to a channel
func (r *groupByExpr) TupleChan(t interface{}) chan<- struct{} {
	cancel := make(chan struct{})
	// reflect on the channel
	chv := reflect.ValueOf(t)
	err := EnsureChan(chv.Type(), r.zero)
	if err != nil {
		r.err = err
		return cancel
	}
	if r.err != nil {
		chv.Close()
		return cancel
	}

	// figure out the new elements used for each of the derived types
	e1 := reflect.TypeOf(r.source1.Zero())

	// create the channel of tuples from source
	// TODO(jonlawlor): restrict the channel direction
	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, e1), 0)
	bcancel := r.source1.TupleChan(body.Interface())

	// for each of the tuples, extract the group values out and set
	// the ones that are not in vtup to the values in the tuple.
	// then, if the tuple does not exist in the groupMap, create a
	// new channel and launch a new goroutine to consume the channel,
	// increment the waitgroup, and finally send the vtup to the
	// channel.

	go func(body, res reflect.Value) {

		// create the map for channels to grouping goroutines
		groupMap := make(map[interface{}]reflect.Value)

		// create waitgroup that indicates that the computations are complete
		var wg sync.WaitGroup

		// figure out where in each of the structs the group and value
		// attributes are found
		e2 := reflect.TypeOf(r.zero) // type of the resulting relation's tuples
		ev := r.valType              // type of the tuples put into groupby values
		e2fieldMap := FieldMap(e1, e2)
		evfieldMap := FieldMap(e1, ev)

		// map from the values to the group (with zeros in the value fields)
		// I couldn't figure out a way to assign the values into the group
		// by modifying it using reflection though so we end up allocating a
		// new element.
		er := r.resType
		rgfieldMap := FieldMap(e2, er)

		// create the select statement reflections
		sourceSel := reflect.SelectCase{reflect.SelectRecv, body, reflect.Value{}}
		canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
		inCases := []reflect.SelectCase{canSel, sourceSel}

		for {
			chosen, tup, ok := reflect.Select(inCases)
			// cancel has been closed, so close the results
			if chosen == 0 {
				close(bcancel)
				return
			}
			if !ok {
				// source channel was closed
				break
			}

			// this reflection may be a bottleneck, and we may be able to
			// replace it with a parallel version.
			gtup, vtup := PartialProject(tup, e2, ev, e2fieldMap, evfieldMap)
			gtupi := gtup.Interface()
			if _, exists := groupMap[gtupi]; !exists {
				// a new group has been encountered
				wg.Add(1)
				// create the channel
				groupChan := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, ev), 0)
				groupMap[gtupi] = groupChan

				// launch a goroutine which consumes values from the group,
				// applies the grouping function, and then when all values
				// are sent, gets the result from the grouping function and
				// puts it into the result tuple, which it then returns

				go func(gtup, groupChan reflect.Value) {
					defer wg.Done()
					// run the grouping function and turn the result into the
					// reflect.Value
					resSel := reflect.SelectCase{reflect.SelectSend, res, reflect.Value{}}

					vals := r.gfcn.Call([]reflect.Value{groupChan})
					// combine the returned values with the group tuple
					// to create the new complete tuple
					resSel.Send = CombineTuples(gtup, vals[0], e2, rgfieldMap)

					_, _, _ = reflect.Select([]reflect.SelectCase{canSel, resSel})
					// we actually don't care about what's been chosen or what
					// happens, just that someone heard an answer or the cancel
					// channel was closed.
				}(gtup, groupChan)
			}
			groupMap[gtupi].Send(vtup)
		}
		// close all of the group channels so the processes can finish
		// this can only be done after the tuples in the original relation
		// have all been sent to the groups
		for _, v := range groupMap {
			v.Close()
		}
		// close the results channel when the waitgroup is finished
		wg.Wait()
		select {
		case <-cancel:
			return
		default:
			if err := r.source1.Err(); err != nil {
				r.err = err
			}
			res.Close()
		}
	}(body, chv)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *groupByExpr) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *groupByExpr) CKeys() CandKeys {
	// determine the new candidate keys, which can be any of the original
	// candidate keys that are a subset of the group (which would also
	// mean that every tuple in the original relation is in its own group
	// in the resulting relation, which means the groupby function was
	// just a map) or the group itself.

	e1 := reflect.TypeOf(r.source1.Zero())
	e2 := r.resType // type of the resulting relation's tuples
	ev := r.valType // type of the tuples put into groupby values

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
	e2fieldMap := FieldMap(e1, e2)
	evfieldMap := FieldMap(e1, ev)

	groupFieldMap := make(map[Attribute]FieldIndex)
	for name, v := range e2fieldMap {
		if _, isValue := evfieldMap[name]; !isValue {
			groupFieldMap[name] = v
		}
	}
	names := FieldNames(e2)

	ck2 := SubsetCandidateKeys(r.source1.CKeys(), names, groupFieldMap)

	// determine the new names and types
	cn := FieldNames(e2)

	if len(ck2) == 0 {
		ck2 = append(ck2, cn)
	}

	return ck2
}

// GoString returns a text representation of the Relation
func (r *groupByExpr) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *groupByExpr) String() string {
	h := FieldNames(r.resType)
	s := make([]string, len(h))
	for i, v := range h {
		s[i] = string(v)
	}

	// TODO(jonlawlor) add better identification to the grouping func,
	// maybe by using runtime.FuncForPC
	return r.source1.String() + ".GroupBy({" + HeadingString(r) + "}->{" + strings.Join(s, ", ") + "})"

}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *groupByExpr) Project(z2 interface{}) Relation {
	return NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
func (r1 *groupByExpr) Restrict(p Predicate) Relation {
	// TODO(jonlawlor): this can be passed through if the predicate only
	// depends upon the attributes that are not in valZero
	return NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
func (r1 *groupByExpr) Rename(z2 interface{}) Relation {
	return NewRename(r1, z2)
}

// Union creates a new relation by unioning the bodies of both inputs
func (r1 *groupByExpr) Union(r2 Relation) Relation {
	return NewUnion(r1, r2)
}

// Diff creates a new relation by set minusing the two inputs
func (r1 *groupByExpr) Diff(r2 Relation) Relation {
	// TODO(jonlawlor): this can be rewritten if there are candidate keys
	// in the groupby are a superset of some candidate keys in the union?
	return NewDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
func (r1 *groupByExpr) Join(r2 Relation, zero interface{}) Relation {
	return NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
func (r1 *groupByExpr) GroupBy(t2, gfcn interface{}) Relation {
	return NewGroupBy(r1, t2, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *groupByExpr) Map(mfcn interface{}, ckeystr [][]string) Relation {
	return NewMap(r1, mfcn, ckeystr)
}

// Err returns an error encountered during construction or computation
func (r1 *groupByExpr) Err() error {
	return r1.err
}

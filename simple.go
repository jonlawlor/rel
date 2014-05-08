// the Simple type is a relation with underlying data stored in a struct
// slice.  It is intended to be a starter implementation that can be used
// to check the validity of more complicated approaches.
//
// It makes heavy use of reflection, but should provide some interesting
// ways of programming in go.  Because it uses so much reflection, it is
// difficult to implement in an idiomatic way.  Also, the performance
// leaves something to be desired!  However, once the interface is complete
// it might be possible to implement it in more efficient ways.
//

package rel

import (
	"reflect"
	"sync"
)

// Simple is an implementation of Relation using a []struct
type Simple struct {
	// Names & Types constitute the heading of the relation.
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to
	// use the methods defined on a particular struct.
	Names []string
	Types []reflect.Type

	// I wish there was a more precise way of representing this?
	Body []interface{}

	// set of candidate keys
	CKeys CandKeys

	// the type of the tuples contained within the relation
	tupleType reflect.Type
}

// Deg returns the degree of the relation
func (r Simple) Deg() int {
	return len(r.Names)
}

// Card returns the cardinality of the relation
func (r Simple) Card() int {
	return len(r.Body)
}

// Tuples sends each tuple in the relation to a channel
func (r Simple) Tuples(t chan interface{}) {
	go func() {
		defer close(t)
		for _, tup := range r.Body {
			t <- tup
		}
	}()
	return
}

// Heading returns a map from column names to types
func (r Simple) Heading() []Attribute {
	deg := r.Deg()
	h := make([]Attribute, deg)
	for i := 0; i < deg; i++ {
		h[i] = Attribute{r.Names[i], r.Types[i]}
	}
	return h
}

// interfaceHeading returns a map from column names to types for an
// input interface
func interfaceHeading(i interface{}) []Attribute {
	Names, Types := namesAndTypes(reflect.TypeOf(i))
	h := make([]Attribute, len(Names))
	for i := 0; i < len(Names); i++ {
		h[i] = Attribute{Names[i], Types[i]}
	}
	return h
}

// String returns a text representation of the Relation
func (r Simple) GoString() string {
	return goStringTabTable(r)
}

func (r Simple) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subset of the current tuple's
// type.  We can't use a slice of strings because go can't construct
// arbitrary types through reflection.
func (r1 Simple) Project(t2 interface{}) (r2 Relation) {
	c := r1.Card()
	b2 := make([]interface{}, c)

	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e2 := reflect.TypeOf(t2)
	if r1.tupleType.AssignableTo(e2) {
		for i, tup := range r1.Body {
			b2[i] = tup
		}
		return Simple{r1.Names, r1.Types, b2, r1.CKeys, e2}
	}

	// figure out which fields stay, and where they are in each of
	// the tuple types.
	fMap := fieldMap(r1.tupleType, e2)
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.

	// figure out which of the candidate keys (if any) to keep.
	// only the keys that only have attributes in the new type are
	// valid.  If we do have any keys that are still valid, then
	// we don't have to perform distinct on the body again.

	// We might want to assign the results of the project to either a map
	// so that it can be re-distincted, or to a []reflect.Value if it is
	// already distinct, based on the results of the candidate Key change
	ck2 := subsetCandidateKeys(r1.CKeys, r1.Names, fMap)

	// assign fields from the old relation to fields in the new
	// TODO(jonlawlor): make this concurrent
	for i, tup1 := range r1.Body {
		tup2 := reflect.Indirect(reflect.New(e2))
		rtup1 := reflect.ValueOf(tup1)
		for _, fm := range fMap {
			tupf2 := tup2.Field(fm.j)
			tupf2.Set(rtup1.Field(fm.i))
		}
		// set the field in the new tuple to the value
		// from the old one
		b2[i] = tup2.Interface()
	}

	// figure out the names to remove from the original data
	cn, ct := namesAndTypes(e2)
	if len(ck2) == 0 {
		// create a new primary key
		// I'm not sure this implementation has good
		// performance.
		m := make(map[interface{}]struct{})
		for _, tup2 := range b2 {
			m[tup2] = struct{}{}
		}
		b2 = make([]interface{}, len(m))
		i := 0
		for tup2 := range m {
			b2[i] = tup2
			i++
		}
		ck2 = append(ck2, []string{})
		copy(ck2[0], cn)
	}
	// construct the returned relation
	return Simple{cn, ct, b2, ck2, e2}
}

// rename operation
// the way this is done has to do a rename in place, so at this point
// the order of the fields becomes significant.  There will only be a
// single input, which should line up with the order and types of the
// fields in the original data.  This will probably be combined with
// project to create a "select xxx as yyy" idiom.
// The advantage of this syntax in go is that the rename can't express
// duplicate attributes, and also the renamed tuple is the new type
// used in type assertions.
func (r1 Simple) Rename(t2 interface{}) (r2 Relation) {
	// TODO(jonlawlor) add a check that the second interface's type is
	// the same as the first, except that it has different names for
	// the same fields.
	c := r1.Card()
	b2 := make([]interface{}, c)

	// first figure out if the tuple types of the relation and rename
	// are equal.  If so, convert the tuples to the (possibly new)
	// type and then return the new relation.
	e2 := reflect.TypeOf(t2)
	if r1.tupleType.AssignableTo(e2) {
		for i, tup := range r1.Body {
			b2[i] = tup
		}
		return Simple{r1.Names, r1.Types, b2, r1.CKeys, e2}
	}

	// assign the values of the original to the new names in the same
	// locations
	n := reflect.ValueOf(t2).NumField()
	for i, tup1 := range r1.Body {
		tup2 := reflect.Indirect(reflect.New(e2))
		rtup1 := reflect.ValueOf(tup1)
		for j := 0; j < n; j++ {
			tupf2 := tup2.Field(j)
			tupf2.Set(rtup1.Field(j))
		}
		b2[i] = tup2.Interface()
	}

	// figure out the new names
	names2 := make([]string, n)
	for i := 0; i < n; i++ {
		f := e2.Field(i)
		names2[i] = f.Name
	}

	// create a map from the old names to the new names if there is
	// any difference between them
	nameMap := make(map[string]string)
	for i, name := range r1.Names {
		nameMap[name] = names2[i]
	}

	// for each of the candidate keys, rename any keys from the old
	// names to the new ones
	ck2 := make([][]string, len(r1.CKeys))
	for i := 0; i < len(ck2); i++ {
		copy(ck2[i], r1.CKeys[i])
		for j, key := range ck2[i] {
			ck2[i][j] = nameMap[key]
		}
	}

	ct := make([]reflect.Type, len(r1.Types))
	copy(ct, r1.Types)

	return Simple{names2, ct, b2, ck2, e2}
}

// union is a set union of two relations
func (r1 Simple) Union(r2 Relation) Relation {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	// turn the first relation into a map and then add on the values from
	// the second one, then return the keys as a new relation

	// for some reason the map requires this to use an Interface() call.
	// maybe there is a better way?

	m := make(map[interface{}]struct{}, r1.Card()+r2.Card())
	for _, tup1 := range r1.Body {
		m[tup1] = struct{}{}
	}

	// the second relation has to return its values through a channel
	tups := make(chan interface{})
	r2.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?  Also, it might be possible to make 2 maps instead of a single
	// map, and populate them concurrently, and at the end merge them.
	// It may be more efficient to store the relation bodies in maps if we
	// always have to construct a map to do anything with them.
	for tup2 := range tups {
		m[tup2] = struct{}{}
	}
	b := make([]interface{}, len(m))
	i := 0
	for tup, _ := range m {
		b[i] = tup
		i++
	}
	// return the new relation
	// TODO(jonlawlor): should these be copies?
	return Simple{r1.Names, r1.Types, b, r1.CKeys, r1.tupleType}
}

// setdiff returns the set difference of the two relations
func (r1 Simple) SetDiff(r2 Relation) (onlyr1 Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.
	m := make(map[interface{}]struct{}, r1.Card())
	for _, tup1 := range r1.Body {
		m[tup1] = struct{}{}
	}

	// the second relation has to return its values through a channel
	tups := make(chan interface{})
	r2.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	for tup2 := range tups {
		delete(m, tup2)
	}
	b := make([]interface{}, len(m))
	i := 0
	for tup, _ := range m {
		b[i] = tup
		i++
	}

	// return the new relation
	// TODO(jonlawlor): should these be copies?
	onlyr1 = Simple{r1.Names, r1.Types, b, r1.CKeys, r1.tupleType}
	return
}

// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
//
// the implementation for Restrict creates a set of go routines to handle
// the application of the predicate, then feeds that set of worker routines
// the tuples from the relation.  Those workers apply the predicate, and if
// it is true, send the tuple to the result chan.  When all of the values
// are consumed, the workers each send a done signal to another go routine,
// which, when all of the workers have finished, closes the result channel.
// The result channel is accumulated into a new relation body and that body
// is used to construct a new relation.
func (r1 Simple) Restrict(p Predicate) Relation {

	// figure out how many items we want to handle at the same time
	mc := MaxConcurrent
	if mc > r1.Card() {
		// if the relation has fewer tuples than the maximum amount
		// we can handle concurrently, only create buffers and go
		// routines for each of the tuples, to save on memory.
		mc = r1.Card()
	}
	// channel of the input tuples
	tups := make(chan interface{}, mc)
	r1.Tuples(tups)

	// channel of the output tuples
	res := make(chan interface{}, mc)

	// done is used to signal when each of the worker goroutines
	// finishes processing the predicates
	done := make(chan struct{})
	go func() {
		for i := 0; i < mc; i++ {
			<-done
		}
		close(res)
	}()

	// create the worker routines, have them evaluate the predicate
	// and if it is true, pass the tuple on to the results stream
	// when all of the input tuples are consumed, send an empty message
	// to the done channel, which will close res when all of the workers
	// have finished.
	for i := 0; i < mc; i++ {
		go func() {
			for tup := range tups {
				if p(tup) {
					res <- tup
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
	return Simple{r1.Names, r1.Types, b, r1.CKeys, r1.tupleType}
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

func (r1 Simple) GroupBy(t2 interface{}, vt interface{}, gfcn func(chan interface{}) interface{}) (r2 Relation) {
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

// partialProject takes the attributes of the input tup, and then for the
// attributes that are in ltyp but not in rtyp, put those values into ltup,
// and put zero values into ltup for the values that are in rtyp.  For the
// rtup, put only values which are in rtyp.
// The reason we have to put zero values is that we can't make derived types.
// returns the results as an interface instead of as reflect.Value's
func partialProject(tup reflect.Value, ltyp, rtyp reflect.Type, lFieldMap, rFieldMap map[string]fieldIndex) (ltupi interface{}, rtupi interface{}) {

	// we could avoid passing in th lFieldMap and

	// assign fields from the old relation to fields in the new
	ltup := reflect.Indirect(reflect.New(ltyp))
	rtup := reflect.Indirect(reflect.New(rtyp))

	// note thet rtup is a subset of ltup, but the fields in ltup that are
	// in ltup will retain the zero value

	for lname, lfm := range lFieldMap {
		// if it is in the right tuple, assign it to the right tuple, otherwise
		// assign it to the left tuple
		if rfm, exists := rFieldMap[lname]; exists {
			tupf := rtup.Field(rfm.j)
			tupf.Set(tup.Field(rfm.i))
		} else {
			tupf := ltup.Field(lfm.j)
			tupf.Set(tup.Field(lfm.i))
		}
	}
	ltupi = ltup.Interface()
	rtupi = rtup.Interface()
	return
}

// combineTuples takes the values in rtup and assigns them to the fields
// in ltup with the same names
func combineTuples(ltup reflect.Value, rtup reflect.Value, ltyp reflect.Type, fMap map[string]fieldIndex) reflect.Value {
	// for some reason I can't get reflect to work on a pointer to an interface
	// so this will use a new ltup and then assign values to it from either the
	// ltup or rtup inputs
	// TODO(jonlawlor): avoid this new allocation somehow
	tup2 := reflect.Indirect(reflect.New(ltyp))
	for i := 0; i < ltyp.NumField(); i++ {
		lf := tup2.Field(i)
		if fm, isRight := fMap[ltyp.Field(i).Name]; isRight {
			// take the values from the right
			lf.Set(rtup.Field(fm.j))
		} else {
			lf.Set(ltup.Field(i))
		}
	}
	return tup2
}

func combineTuples2(to *reflect.Value, from reflect.Value, fMap map[string]fieldIndex) {
	// for some reason I can't get reflect to work on a pointer to an interface
	// so this will use a new ltup and then assign values to it from either the
	// ltup or rtup inputs
	// TODO(jonlawlor): avoid this new allocation somehow
	for _, fm := range fMap {
		tof := to.Field(fm.i)
		tof.Set(from.Field(fm.j))
	}
	return
}

// Join is the natural join operation
func (r1 Simple) Join(r2 Relation, t3 interface{}) (r3 Relation) {

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

func partialEquals(tup1 reflect.Value, tup2 reflect.Value, fmap map[string]fieldIndex) bool {
	for _, fm := range fmap {
		if tup1.Field(fm.i).Interface() != tup2.Field(fm.j).Interface() {
			return false
		}
	}
	return true
}

func subsetCandidateKeys(cKeys1 [][]string, names1 []string, fMap map[string]fieldIndex) [][]string {

	remNames := make(map[string]struct{})
	for _, n1 := range names1 {
		if _, keyfound := fMap[n1]; !keyfound {
			remNames[n1] = struct{}{}
		}
	}

	cKeys2 := make([][]string, 0)
KeyLoop:
	for _, ck := range cKeys1 {
		// if the candidate key contains a name we want to remove, then
		// get rid of it
		for _, k := range ck {
			if _, keyfound := remNames[k]; keyfound {
				continue KeyLoop
			}
		}
		cKeys2 = append(cKeys2, ck)
	}
	return cKeys2
}

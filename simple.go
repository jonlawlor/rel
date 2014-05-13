// the Simple type is a relation with underlying data stored in a channel.
//  It is intended to be a starter implementation that can be used
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
	// heading constitute the heading of the relation.
	// using slices here instead of a map to preserve order
	// the reason is because golang distinguishes between structs
	// based on the order of their fields, and users may want to
	// use the methods defined on a particular struct.
	heading []Attribute

	// the channel of tuples in the relation
	body chan T

	// set of candidate keys
	cKeys CandKeys

	// the type of the tuples contained within the relation
	zero T
}

// New creates a new Relation.
// it returns a Relation implemented using the Simple
// structure, which keeps Tuples in a single channel.  We may want to
// change this to be more flexible with how relations are represented.
func New(v interface{}, ckeystr [][]string) *Simple {
	// TODO(jonlawlor): allow callers to provide different inputs,
	// like map[struct{...}]struct{} or []struct{...} which could also
	// represent a relation, and also error out if we can't figure out
	// how to construct a relation from the input.
	// There should also be a way to construct a relation with an input
	// that you already know is distinct, so we don't have to ensure it
	// ourselves.
	
	// right now v can ONLY be a channel of the appropriate type.
	// we can probably also accept a slice of values or a map, or pointers
	// to that kind
	r := new(Simple)

	rChan := refect.ValueOf(v)
	// create the body
	r.body = make(chan T)
	go func(body chan T) {
		for {
			// this will always attempt to pull at least one value
			val, ok := rChan.Recv()
			if !ok {
				break
			}
			body <- val.Interface()
		}
		close(body)
	}(r.body)

	r.cKeys = CandKeys(ckeystr)

	// create zero element
	r.zero = rChan.Elem().Interface()

	// create the heading
	cn, ct := namesAndTypes(rChan.Elem())
	r.heading := make([]Attribute, len(cn))
	for i := 0; i < len(cn); i++ {
		r.heading[i] = Attribute{cn[i], ct[i]}
	}
	
	// ensure minimal candidate keys
	if len(r.cKeys) == 0 {
		r.body = distinct(r.body)

		// all relations have a candidate key of all of their attributes, or
		// a non zero subset if the relation is not dee or dum
		r.cKeys = append(r.cKeys, cn)
		
		// change the body to use a distinct channel instead of an assumed
		// distinct channel
	}
	
	// we might want to check the candidate keys for validity here?
	orderCandidateKeys(r.cKeys)
	return r
}


// Deg returns the degree of the relation
func (r *Simple) Deg() int {
	return len(r.heading)
}

// Heading returns a map from column names to types
func (r *Simple) Heading() []Attribute {
	return r.heading
}

// Card returns the cardinality of the relation
// note: this consumes the values of the relation
func (r *Simple) Card() i int {	
	for _ := range(r.body) {
		i++
	}
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
// typical usage is probably
// go r.Tuples(chanVal)
func (r *Simple) Tuples(t chan T) {
	for v := range(r.body) {
		t <- v
	}
	close(t)
}


// Zero returns the zero value of the relation (a blank tuple)
func (r *Simple) Zero() T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *Simple) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *Simple) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *Simple) String() string {
	return stringTabTable(r)
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subset of the current tuple's
// type.  We can't use a slice of strings because go can't construct
// arbitrary types through reflection.
func (r *Simple) Project(z2 T) {
	
	// transform the channel of tuples from the relation
	z1 := r1.Zero()

	// first figure out if the tuple types of the relation and
	// projection are equivalent.  If so, convert the tuples to
	// the (possibly new) type and then return the new relation.
	e1 := reflect.TypeOf(r1.Zero())
	e2 := reflect.TypeOf(z2)

	if e1.AssignableTo(e2) {
		// nothing to do, I think.
		return
	}

	// figure out which fields stay, and where they are in each of
	// the tuple types.
	fMap := fieldMap(e1, e2)
	// TODO(jonlawlor): error if fields in e2 are not in r1's tuples.

	// assign fields from the old relation to fields in the new
	body2 := make(chan T)
	go func(body chan T) {
		for tup1 := range body {
			tup2 := reflect.Indirect(reflect.New(e2))
			rtup1 := reflect.ValueOf(tup1)
			for _, fm := range fMap {
				tupf2 := tup2.Field(fm.j)
				tupf2.Set(rtup1.Field(fm.i))
			}
			// set the field in the new tuple to the value
			// from the old one
			body2 <- tup2.Interface()
		}
		close(body2)
	}(r.body)
	
	// figure out which of the candidate keys (if any) to keep.
	// only the keys that only have attributes in the new type are
	// valid.  If we do have any keys that are still valid, then
	// we don't have to perform distinct on the body.

	cn1 := make([]string, len(r.heading))
	for i, att := range r.heading {
		cn1[i] = att.Name
	}
	r.cKeys := subsetCandidateKeys(r.cKeys, cn1, fMap)
	cn2, ct2 := namesAndTypes(e2)
	
	if len(cn2) == 0 {
		// make a new primary key and ensure the results are distinct
		r.cKeys = append(r.cKeys, cn2)
		r.body = distinct(body2)
	} else {
		r.body = body2
	}

	// create the new heading
	// TODO(jonlawlor): we can actually reuse memory here because the project
	// can't be any bigger than the original
	r.heading := make([]Attribute, len(cn2))
	for i := 0; i < len(cn2); i++ {
		r.heading[i] = Attribute{cn2[i], ct2[i]}
	}
}


// Restrict applies a predicate to a relation and returns a new relation
// Predicate is a func which accepts an interface{} of the same dynamic
// type as the tuples of the relation, and returns a boolean.
func (r *Simple) Restrict(p Predicate) {
	// take the internal channel and apply a predicate to it

	// channel of the output tuples
	body2 := make(chan T)

	// transform the body so that it only sends values that pass the
	// predicate
	go func(body chan T) {
		for tup := range body {
			if p(tup) {
				body2 <- tup
			}
		}
		close(body2)
	}()
	
	r.body = body2
}

// rename operation
// the way this is done has to do a rename in place, so at this point
// the order of the fields becomes significant.  There will only be a
// single input, which should line up with the order and types of the
// fields in the original data.
// The advantage of this syntax in go is that the rename can't express
// duplicate attributes, and also the renamed tuple is the new type
// used in type assertions on resulting values.
func (r *Simple) Rename(t2 interface{}) {
	// TODO(jonlawlor) add a check that the second interface's type is
	// the same as the first, except that it has different names for
	// the same fields.

	// first figure out if the tuple types of the relation and rename
	// are equal.  If so, convert the tuples to the (possibly new)
	// type and then return the new relation.
	z1 := reflect.TypeOf(r.zero)
	z2 := reflect.TypeOf(t2)
	if z1.AssignableTo(z2) {
		// do nothing
		return
	}

	body2 := make(chan T)
	// assign the values of the original to the new names in the same
	// locations
	n := reflect.ValueOf(z2).NumField()

	go func(body chan T) {
		for tup1 := range r1.Body {
			tup2 := reflect.Indirect(reflect.New(e2))
			rtup1 := reflect.ValueOf(tup1)
			for i := 0; i < n; i++ {
				tupf2 := tup2.Field(i)
				tupf2.Set(rtup1.Field(i))
			}
			body2 <- tup2.Interface()
		}
		close(body2)
	}(r.body)
	r.body = body2

	// figure out the new names
	names2 := make([]string, n)
	for i := 0; i < n; i++ {
		f := e2.Field(i)
		names2[i] = f.Name
	}

	// create a map from the old names to the new names if there is
	// any difference between them
	nameMap := make(map[string]string)
	for i, att := range r.heading {
		nameMap[att.Name] = names2[i]
	}

	// for each of the candidate keys, rename any keys from the old
	// names to the new ones
	for i := 0; i < len(r.cKeys); i++ {
		for j, key := range r.cKeys[i] {
			r.cKeys[i][j] = nameMap[key]
		}
	}
	
	// change the heading
	for i := 0; i < len(r.heading); i++ {
		r.heading[i].Name = names2[i]
	}
	
}

// union is a set union of two relations
func (r1 *Simple) Union(r2 *Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.

	// turn the first relation into a map and then add on the values from
	// the second one, then return the keys as a new relation

	// for some reason the map requires this to use an Interface() call.
	// maybe there is a better way?

	var mu sync.Mutex
	m := make(map[interface{}]struct{})
	
	body2 := make(chan T)
	go r2.Tuples(body2)
	
	res := make(chan T)
	
	done := make(chan struct{})
	// function to handle closing of the results channel
	go func() {
		// one for each body
		<-done
		<-done
		close(res)
	}
	
	combine := func(body chan T) {
		for tup := range body {
			mu.Lock()
			if _, dup := m[tup]; !dup {
				m[tup] = struct{}{}
				mu.Unlock()
				res <- tup
			} else {
				mu.Unlock()
			}
		}
		done <- struct{}{}
		return
	}
	go combine(r1.body)
	go combine(body2)
	
	r1.body = res
}

// setdiff returns the set difference of the two relations
func (r1 *Simple) SetDiff(r2 *Relation) {
	// TODO(jonlawlor): check that the two relations conform, and if not
	// then panic.
	
	m := make(map[interface{}]struct{})
	// setdiff is unique in that it has to immediately consume all of the
	// values from the second relation in order to send any values in the
	// first one.  All other relational operations can be done lazily, this
	// one can only be done half-lazy.
	// with some indexing this is avoidable.
	
	// second set of tups
	body2 := make(chan T)
	r2.Tuples(body2)
	
	res := make(chan T)
	
	go func (b1, b2 chan T) {
		for tup := range(b2) {
			m[tup] = struct{}{}
		}
		for tup := range(b1) {
			if _, rem := m[tup]; !rem {
				res <- tup
			}
		}
		close(res)
	}(r1.body, body2)
	
	r1.body = res
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


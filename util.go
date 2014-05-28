package rel

import (
	"fmt" // we might want to replace this with errors
	"reflect"
	"sort"
)

// fieldIndex is used to map between attributes in different relations
// that have the same name
type fieldIndex struct {
	i int
	j int
}

// fieldNames takes a reflect.Type of a struct and returns field names in order
func fieldNames(e reflect.Type) []Attribute {
	n := e.NumField()
	names := make([]Attribute, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		names[i] = Attribute(f.Name)
	}
	return names
}

// fieldTypes takes a reflect.Type of a struct and returns field types in order
func fieldTypes(e reflect.Type) []reflect.Type {
	n := e.NumField()
	types := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		types[i] = f.Type
	}
	return types
}

func orderCandidateKeys(ckeys CandKeys) {
	// first go through each set of keys and alphabetize
	// this is used to compare sets of candidate keys
	for _, ck := range ckeys {
		str := make([]string, len(ck))
		for i := range ck {
			str[i] = string(ck[i])
		}
		sort.Strings(str)
		for i := range ck {
			ck[i] = Attribute(str[i])
		}
	}

	// then sort by length so that smaller keys are first
	sort.Sort(ckeys)
}

func string2CandKeys(ckeystrs [][]string) CandKeys {
	cks := make([][]Attribute, len(ckeystrs))
	for i, ckstr := range ckeystrs {
		cks[i] = make([]Attribute, len(ckstr))
		for j, str := range ckstr {
			cks[i][j] = Attribute(str)
		}
	}
	return cks
}

// definitions for the candidate key sorting
func (cks CandKeys) Len() int {
	return len(cks)
}
func (cks CandKeys) Swap(i, j int) {
	cks[i], cks[j] = cks[j], cks[i]
}
func (cks CandKeys) Less(i, j int) bool {
	return len(cks[i]) < len(cks[j]) // note this is smallest to largest
}

// distinct changes an interface channel to a channel of unique interfaces
// TODO(jonlawlor): change this to a function that takes a destination chan
// and returns a function which can be used to send values to the destination
// if they have not already been sent.  We might want a mutex as well?
func distinct(b1 chan T) (b2 chan T) {
	m := make(map[interface{}]struct{})
	b2 = make(chan T)
	go func() {
		for v := range b1 {
			if _, dup := m[v]; !dup {
				m[v] = struct{}{}
				b2 <- v
			}
		}
		close(b2)
	}()
	return
}

// checkCandidateKeys checks the set of candidate keys
// this ensures that the names of the keys are all in the attributes
// of the relation
func checkCandidateKeys(ckeys CandKeys, cn []Attribute) (err error) {
	// TODO(jonlawlor) cannonicalize these somehow
	names := make(map[Attribute]struct{})
	for _, n := range cn {
		names[n] = struct{}{}
	}
	for _, ck := range ckeys {
		if len(ck) == 0 {
			// note that this doesn't fire if ckeys is also empty
			// but that is by design
			err = fmt.Errorf("empty candidate key not allowed")
			return
		}
		for _, k := range ck {
			_, keyFound := names[k]
			if !keyFound {
				err = fmt.Errorf("prime attribute not found: %s", k)
				return
			}
		}
	}
	return
}

// fieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func fieldMap(e1, e2 reflect.Type) map[Attribute]fieldIndex {
	// TODO(jonlawlor): we might want to exclude unexported fields?
	fn1 := fieldNames(e1)
	fn2 := fieldNames(e2)
	return attributeMap(fn1, fn2)
}

// fieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func attributeMap(fn1 []Attribute, fn2 []Attribute) map[Attribute]fieldIndex {
	m := make(map[Attribute]fieldIndex)
	for i, n1 := range fn1 {
		for j, n2 := range fn2 {
			if n1 == n2 {
				m[n1] = fieldIndex{i, j}
				break
			}
		}
	}
	return m
}

// partialProject takes the attributes of the input tup, and then for the
// attributes that are in ltyp but not in rtyp, put those values into ltup,
// and put zero values into ltup for the values that are in rtyp.  For the
// rtup, put only values which are in rtyp.
// The reason we have to put zero values is that we can't make derived types.
// returns the results as an interface instead of as reflect.Value's
func partialProject(tup reflect.Value, ltyp, rtyp reflect.Type, lFieldMap, rFieldMap map[Attribute]fieldIndex) (ltupi interface{}, rtupi interface{}) {

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
func combineTuples(ltup, rtup reflect.Value, ltyp reflect.Type, fMap map[Attribute]fieldIndex) reflect.Value {
	// for some reason I can't get reflect to work on a pointer to an interface
	// so this will use a new ltup and then assign values to it from either the
	// ltup or rtup inputs
	// TODO(jonlawlor): avoid this new allocation somehow
	tup2 := reflect.Indirect(reflect.New(ltyp))
	leftNames := fieldNames(ltyp)
	for i, leftName := range leftNames {
		lf := tup2.Field(i)
		if fm, isRight := fMap[leftName]; isRight {
			// take the values from the right
			lf.Set(rtup.Field(fm.j))
		} else {
			lf.Set(ltup.Field(i))
		}
	}
	return tup2
}

func combineTuples2(to *reflect.Value, from reflect.Value, fMap map[Attribute]fieldIndex) {
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

func partialEquals(tup1 reflect.Value, tup2 reflect.Value, fmap map[Attribute]fieldIndex) bool {
	for _, fm := range fmap {
		if tup1.Field(fm.i).Interface() != tup2.Field(fm.j).Interface() {
			return false
		}
	}
	return true
}

// subsetCandidateKeys subsets candidate keys so they only include given fields
func subsetCandidateKeys(cKeys1 [][]Attribute, names1 []Attribute, fMap map[Attribute]fieldIndex) [][]Attribute {

	remNames := make(map[Attribute]struct{})
	for _, n1 := range names1 {
		if _, keyfound := fMap[n1]; !keyfound {
			remNames[n1] = struct{}{}
		}
	}

	cKeys2 := make([][]Attribute, 0)
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

// defaultkey provides the default candidate key for a relation
// This is used when no candidate keys are provided.
// note that this will not be sorted correctly
func defaultKeys(z interface{}) CandKeys {
	// get the names of the fields out of the interface
	e := reflect.TypeOf(z)
	return CandKeys{fieldNames(e)}
}

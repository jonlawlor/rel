package rel

import (
	"reflect"
	"sort"
)

// fieldIndex is used to map between attributes in different relations
// that have the same name
type fieldIndex struct {
	i int
	j int
}

func namesAndTypes(e reflect.Type) ([]string, []reflect.Type) {
	n := e.NumField()
	names := make([]string, n)
	types := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		names[i] = f.Name
		types[i] = f.Type
	}
	return names, types
}

func orderCandidateKeys(ckeys CandKeys) {
	// first go through each set of keys and alphabetize
	// this is used to compare sets of candidate keys
	for _, ck := range ckeys {
		sort.Strings(ck)
	}

	// then sort by length so that smaller keys are first
	sort.Sort(ckeys)
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
func checkCandidateKeys(ckeys CandKeys, cn []string) (err error) {
	// TODO(jonlawlor) cannonicalize these somehow
	names := make(map[string]struct{})
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
func fieldMap(e1 reflect.Type, e2 reflect.Type) map[string]fieldIndex {
	// TODO(jonlawlor): we might want to exclude unexported fields?
	m := make(map[string]fieldIndex)
	for i := 0; i < e1.NumField(); i++ {
		n1 := e1.Field(i).Name
		// find the field location in the original tuples
		for j := 0; j < e2.NumField(); j++ {
			n2 := e2.Field(j).Name
			if n1 == n2 {
				m[n1] = fieldIndex{i, j}
				break
			}
		}
	}
	return m
}

// fieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func attributeMap(h1 []Attribute, h2 []Attribute) map[string]fieldIndex {
	m := make(map[string]fieldIndex)
	for i := 0; i < len(h1); i++ {
		n1 := h1[i].Name
		// find the field location in the other heading
		for j := 0; j < len(h2); j++ {
			if n1 == h2[j].Name {
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

func partialEquals(tup1 reflect.Value, tup2 reflect.Value, fmap map[string]fieldIndex) bool {
	for _, fm := range fmap {
		if tup1.Field(fm.i).Interface() != tup2.Field(fm.j).Interface() {
			return false
		}
	}
	return true
}

// subsetCandidateKeys subsets candidate keys so they only include given fields
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

// defaultkey provides the default candidate key for a relation
// This is used when no candidate keys are provided.
func defaultKeys(z interface{}) CandKeys {
	// get the names of the fields out of the interface
	e := reflect.TypeOf(z)
	ck := make([]string, e.NumField())
	for i := 0; i < e.NumField(); i++ {
		ck[i] = e.Field(i).Name
	}
	sort.Strings(ck)
	return CandKeys{ck}
}

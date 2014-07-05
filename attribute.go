// attributes, and the candidate keys and predicates constructed from
// attributes.  It also contains the definitions of tuples.

package rel

import (
	"reflect"
	"sort"
)

// Attribute represents a particular attribute's name in a relation
type Attribute string

// CandKeys is a set of candidate keys
// they should be unique and sorted
type CandKeys [][]Attribute

// FieldIndex is used to map between attributes in different relations
// that have the same name
type FieldIndex struct {
	I int
	J int
}

// FieldNames takes a reflect.Type of a struct and returns field names in order
func FieldNames(e reflect.Type) []Attribute {
	n := e.NumField()
	names := make([]Attribute, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		names[i] = Attribute(f.Name)
	}
	return names
}

// FieldTypes takes a reflect.Type of a struct and returns field types in order
func FieldTypes(e reflect.Type) []reflect.Type {
	n := e.NumField()
	types := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		f := e.Field(i)
		types[i] = f.Type
	}
	return types
}

// OrderCandidateKeys sorts candidate keys by number of attributes and then alphabetically.
func OrderCandidateKeys(ckeys CandKeys) {
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

// String2CandKeys converts a slice of string slices into a set of candidate keys.
// The result is not sorted, so OrderCandidateKeys should be called afterwards if the
// input is not already sorted in the same way.
func String2CandKeys(ckeystrs [][]string) CandKeys {
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

// Less compares two candidate keys
func less(ck1 []Attribute, ck2 []Attribute) bool {
	if len(ck1) == len(ck2) {
		// alphabetical ordering
		for k := range ck1 {
			if ck1[k] < ck2[k] {
				return true
			} else if ck1[k] > ck2[k] {
				return false
			}
		}
		return false
	}
	if len(ck1) < len(ck2) {
		return true
	}
	return false
}

func (cks CandKeys) Less(i, j int) bool {
	// note this is smallest to largest
	return less(cks[i], cks[j])
}

// DefaultKeys provides the default candidate key for a relation
// This is used when no candidate keys are provided.
// note that this will not be sorted correctly
func DefaultKeys(z interface{}) CandKeys {
	// get the names of the fields out of the interface
	e := reflect.TypeOf(z)
	return CandKeys{FieldNames(e)}
}

// SubsetCandidateKeys subsets candidate keys so they only include given fields
func SubsetCandidateKeys(cKeys1 [][]Attribute, names1 []Attribute, fMap map[Attribute]FieldIndex) [][]Attribute {

	remNames := make(map[Attribute]struct{})
	for _, n1 := range names1 {
		if _, keyfound := fMap[n1]; !keyfound {
			remNames[n1] = struct{}{}
		}
	}

	var cKeys2 [][]Attribute
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

// FieldMap creates a map from fields of one struct type to the fields of another
// the returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func FieldMap(e1, e2 reflect.Type) map[Attribute]FieldIndex {
	// TODO(jonlawlor): we might want to exclude unexported fields?
	fn1 := FieldNames(e1)
	fn2 := FieldNames(e2)
	return AttributeMap(fn1, fn2)
}

// AttributeMap creates a map from positions of one set of attributes to another.
// The returned map's values have two fields i,j , which indicate the location of
// the field name in the input types
// if the field is absent from either of the inputs, it is not returned.
func AttributeMap(fn1, fn2 []Attribute) map[Attribute]FieldIndex {
	m := make(map[Attribute]FieldIndex)
	for i, n1 := range fn1 {
		for j, n2 := range fn2 {
			if n1 == n2 {
				m[n1] = FieldIndex{i, j}
				break
			}
		}
	}
	return m
}

// IsSubDomain returns true if the attributes in sub are all members of dom, otherwise false
// this would be faster if []Attributes were always ordered
// TODO(jonlawlor): make this a method of []Attribute?
func IsSubDomain(sub, dom []Attribute) bool {
SubLoop:
	for _, n1 := range sub {
		for _, n2 := range dom {
			if n1 == n2 {
				continue SubLoop
			}
		}
		return false
	}
	return true
}

// PartialProject takes the attributes of the input tup, and then for the
// attributes that are in ltyp but not in rtyp, put those values into ltup,
// and put zero values into ltup for the values that are in rtyp.  For the
// rtup, put only values which are in rtyp.
// The reason we have to put zero values is that we can't make derived types.
// returns the results as an interface instead of as reflect.Value's
func PartialProject(tup reflect.Value, ltyp, rtyp reflect.Type, lFieldMap, rFieldMap map[Attribute]FieldIndex) (reflect.Value, reflect.Value) {
	// assign fields from the old relation to fields in the new
	ltup := reflect.Indirect(reflect.New(ltyp))
	rtup := reflect.Indirect(reflect.New(rtyp))

	// note thet rtup is a subset of ltup, but the fields in ltup that are
	// in ltup will retain the zero value
	for lname, lfm := range lFieldMap {
		if _, exists := rFieldMap[lname]; !exists {
			tupf := ltup.Field(lfm.J)
			tupf.Set(tup.Field(lfm.I))
		}
	}
	for _, rfm := range rFieldMap {
		tupf := rtup.Field(rfm.J)
		tupf.Set(tup.Field(rfm.I))
	}
	return ltup, rtup
}

// CombineTuples takes the values in rtup and ltup and creates a new tuple that
// takes fields from the right tuple if possible, otherwise takes fields from
// the left tuple.
func CombineTuples(ltup, rtup reflect.Value, ltyp reflect.Type, fMap map[Attribute]FieldIndex) reflect.Value {
	tup2 := reflect.Indirect(reflect.New(ltyp))
	leftNames := FieldNames(ltyp)
	for i, leftName := range leftNames {
		lf := tup2.Field(i)
		if fm, isRight := fMap[leftName]; isRight {
			// take the values from the right
			lf.Set(rtup.Field(fm.J))
		} else {
			lf.Set(ltup.Field(i))
		}
	}
	return tup2
}

// CombineTuples2 takes the values in from and assigns them to the fields in to
// with the same names.
// TODO(jonlawlor): figure out how to combine with CombineTuples, or rename
// this func.  Very ugly.
func CombineTuples2(to *reflect.Value, from reflect.Value, fMap map[Attribute]FieldIndex) {
	for _, fm := range fMap {
		tof := to.Field(fm.I)
		tof.Set(from.Field(fm.J))
	}
	return
}

// PartialEquals returns true when two tuples have equal values in the attributes
// with the same names.
func PartialEquals(tup1 reflect.Value, tup2 reflect.Value, fmap map[Attribute]FieldIndex) bool {
	for _, fm := range fmap {
		if tup1.Field(fm.I).Interface() != tup2.Field(fm.J).Interface() {
			return false
		}
	}
	return true
}

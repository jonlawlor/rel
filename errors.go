// errors are a set of functions useful for type checks, given the absence
// of static type checking due to reflection.

package rel

import (
	"fmt"
	"reflect"
)

// I've tried to reproduce go's type error strings here, because these errors
// act as a (poor) replacement for static type checking.

// ContainerError represents an error that occurs when the wrong kind of
// container is given to a TupleChan, TupleSlice, or TupleMap method, as
// indicated by the name of the method.
type ContainerError struct {
	Expected reflect.Kind
	Found    reflect.Kind
}

func (e *ContainerError) Error() string {
	return "rel: expected tuple container '" + e.Expected.String() + "', found '" + e.Found.String() + "'"
}

// ElemError represents an error that occurs when the wrong kind of element
// is provided to a container given to TupleChan, TupleSlice, or TupleMap
// methods of relations.
type ElemError struct {
	Expected reflect.Type
	Found    reflect.Type
}

func (e *ElemError) Error() string {
	return "rel: expected tuple element '" + e.Expected.Name() + "', found '" + e.Found.Name() + "'"
}

// EnsureChan returns an error if the the input is not a channel with elements
// of the specified type.
func EnsureChan(ch reflect.Type, zero interface{}) error {
	if t := ch.Kind(); t == reflect.Chan {
		// now check that the zero element can be sent to the channel
		te := ch.Elem()
		ze := reflect.TypeOf(zero)
		if te != ze {
			return &ElemError{ze, te}
		}
		return nil
	} else {
		return &ContainerError{t, reflect.Chan}
	}
}

// EnsureSlice returns an error if the the input is not a slice with elements
// of the specified type.
func EnsureSlice(sl reflect.Type, zero interface{}) error {
	if t := sl.Kind(); t == reflect.Slice {
		// now check that the zero element can be sent to the channel
		te := sl.Elem()
		ze := reflect.TypeOf(zero)
		if te != ze {
			return &ElemError{ze, te}
		}
		return nil
	} else {
		return &ContainerError{t, reflect.Slice}
	}
}

// EnsureMap returns an error if the the input is not a map with key elements
// of the specified type, and value elements of type struct{}
func EnsureMap(m reflect.Type, zero interface{}) error {
	if t := m.Kind(); t == reflect.Map {
		// now check that the zero element can be sent to the channel
		tk := m.Key()
		ze := reflect.TypeOf(zero)
		if tk != ze {
			return &ElemError{ze, tk}
		} else {
			te := m.Elem()
			empty := reflect.TypeOf(struct{}{})
			if te != empty {
				return fmt.Errorf("rel: Non-empty map value type, '%v'", te)
			}
			return nil
		}
	} else {
		return &ContainerError{t, reflect.Map}
	}
}

// funcArityError represents an error that occurs when the wrong number of
// inputs or outputs to a function are provided to groupby or map
type funcArityError struct {
	Expected int
	Found    int
}

// InputFuncArityError represents an error that occurs when the wrong number of
// inputs to a function are provided to groupby or map
type NumInError funcArityError

func (e *NumInError) Error() string {
	return fmt.Sprintf("rel: expected input arity %d, found %d", e.Expected, e.Found)
}

// OutputFuncArityError represents an error that occurs when the wrong number of
// outputs to a function are provided to groupby or map
type NumOutError funcArityError

func (e *NumOutError) Error() string {
	return fmt.Sprintf("rel: expected output arity %d, found %d", e.Expected, e.Found)
}

// domainErorr represents an error that occurs when the input or output tuples
// of a function are not subdomains of the expected domains
type domainError struct {
	Expected []Attribute
	Found    []Attribute
}

// InDomainError represents an error that occurs when the input tuples
// of a function are not subdomains of the expected domain
type InDomainError domainError

func (e *InDomainError) Error() string {
	return fmt.Sprintf("rel: expected function input to be subdomain of %v, found %v", e.Expected, e.Found)
}

// OutDomainError represents an error that occurs when the output tuples
// of a function are not subdomains of the expected domain
type OutDomainError domainError

func (e *OutDomainError) Error() string {
	return fmt.Sprintf("rel: expected function output to be subdomain of %v, found %v", e.Expected, e.Found)
}

// EnsureGroupFunc returns an error if the input is not a function with only
// one input and one output, where the input and output are subdomains of given
// tuples.
func EnsureGroupFunc(gfcn reflect.Type, inSuper, outSuper interface{}) (err error, inTup, outTup reflect.Type) {
	if t := gfcn.Kind(); t != reflect.Func {
		err = &ContainerError{t, reflect.Func}
		return
	}

	if ni := gfcn.NumIn(); ni != 1 {
		err = &NumInError{1, ni}
		return
	}

	ch := gfcn.In(0)
	if t := ch.Kind(); t != reflect.Chan {
		err = &ContainerError{t, reflect.Chan}
		return
	}
	inTup = ch.Elem()

	if no := gfcn.NumOut(); no != 1 {
		err = &NumOutError{1, no}
		return
	}
	outTup = gfcn.Out(0)

	// check that the fields are subdomains
	inDomain := FieldNames(reflect.TypeOf(inSuper))
	if fn := FieldNames(inTup); !IsSubDomain(fn, inDomain) {
		err = &InDomainError{inDomain, fn}
		return
	}

	outDomain := FieldNames(reflect.TypeOf(outSuper))
	if fn := FieldNames(outTup); !IsSubDomain(fn, outDomain) {
		err = &OutDomainError{outDomain, fn}
		return
	}
	return
}

// EnsureMapFunc returns an error if the input is not a function with only
// one input and one output, where the input is a subdomain of given
// tuple.
func EnsureMapFunc(mfcn reflect.Type, inSuper interface{}) (err error, inTup, outTup reflect.Type) {
	if t := mfcn.Kind(); t != reflect.Func {
		err = &ContainerError{t, reflect.Func}
		return
	}

	if ni := mfcn.NumIn(); ni != 1 {
		err = &NumInError{1, ni}
		return
	}
	inTup = mfcn.In(0)

	if no := mfcn.NumOut(); no != 1 {
		err = &NumOutError{1, no}
		return
	}
	outTup = mfcn.Out(0)

	// check that the fields are subdomains
	inDomain := FieldNames(reflect.TypeOf(inSuper))
	if fn := FieldNames(inTup); !IsSubDomain(fn, inDomain) {
		err = &InDomainError{inDomain, fn}
		return
	}
	return
}

// AttributeSubsetError represents an error that occurs when a method on a
// relation is called with a set of tuples that are not a subset of an expected
// type.
type AttributeSubsetError domainError

func (e *AttributeSubsetError) Error() string {
	return fmt.Sprintf("rel: expected attributes to be a subset of %v, found %v", e.Expected, e.Found)
}

// EnsureSubDomain returns an error if the input sub is not a subdomain of
// input dom.
func EnsureSubDomain(sub, dom []Attribute) (err error) {
	if IsSubDomain(sub, dom) {
		return
	}
	// figure out the attributes that are in sub that are not in dom
	invalidAttributes := make([]Attribute, 1)
SubLoop:
	for _, n1 := range sub {
		for _, n2 := range dom {
			if n1 == n2 {
				continue SubLoop
			}
		}
		invalidAttributes = append(invalidAttributes, n1)
	}
	return &AttributeSubsetError{dom, invalidAttributes}
}

// DegreeError represents an error that occurs when the input tuples to a
// relational operation do not have the same degree as expected.  This only
// occurs in rename operations.
type DegreeError struct {
	Expected int
	Found    int
}

func (e *DegreeError) Error() string {
	return fmt.Sprintf("rel: expected degree %d, found %d", e.Expected, e.Found)
}

// DomainMismatchError represents an error that occurs when two tuples have a
// different set of attributes.
type DomainMismatchError domainError

func (e *DomainMismatchError) Error() string {
	return fmt.Sprintf("rel: mismatched domains found: %v, and %v", e.Expected, e.Found)
}

// EnsureSameDomain returns an error if the inputs do not have the same domain.
func EnsureSameDomain(sub, dom []Attribute) (err error) {
	if len(sub) == len(dom) && IsSubDomain(sub, dom) {
		return
	}
	// sub and dom do not have any particular order.
	return &DomainMismatchError{sub, dom}
}

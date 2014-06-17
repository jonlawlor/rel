package att

import (
	"fmt"
	"reflect"
	"testing"
)

// tests & benchmarks for Predicates

// type of the example tuples
type exTup2 struct {
	Foo int
	Bar string
}

func exTup2Func(ex exTup2) bool {
	return true
}

func TestStringer(t *testing.T) {

	Foo := Attribute("Foo")
	Bar := Attribute("Bar")
	var predTests = []struct {
		in  fmt.Stringer
		out string
	}{
		{Foo.EQ(Bar), "Foo == Bar"},
		{Foo.EQ("Bar"), "Foo == Bar"},
		{Foo.NE(Bar), "Foo != Bar"},
		{Foo.NE("Bar"), "Foo != Bar"},
		{Foo.LT(Bar), "Foo < Bar"},
		{Foo.LT("Bar"), "Foo < Bar"},
		{Foo.LE(Bar), "Foo <= Bar"},
		{Foo.LE("Bar"), "Foo <= Bar"},
		{Foo.GT(Bar), "Foo > Bar"},
		{Foo.GT("Bar"), "Foo > Bar"},
		{Foo.GE(Bar), "Foo >= Bar"},
		{Foo.GE("Bar"), "Foo >= Bar"},
		{Foo.EQ(Bar), "Foo == Bar"},
		{Foo.EQ("Bar"), "Foo == Bar"},
		{AdHoc{func(ex exTup2) bool { return true }}, "func({Foo, Bar})"},
		{AdHoc{exTup2Func}, "func({Foo, Bar})"},

		{Foo.EQ(Bar).And(Foo.NE(Bar)), "(Foo == Bar) && (Foo != Bar)"},
		{Foo.EQ(Bar).Or(Foo.NE(Bar)), "(Foo == Bar) || (Foo != Bar)"},
		{Foo.EQ(Bar).Xor(Foo.NE(Bar)), "(Foo == Bar) != (Foo != Bar)"},
	}
	for _, tt := range predTests {
		s := tt.in.String()
		if s != tt.out {
			t.Errorf("String() => %v, want %v", tt.in, tt.out)
		}
	}

}

// tests EvalFunc and predicate composition
func TestEvalFunc(t *testing.T) {
	True := AdHoc{func(ex exTup2) bool {
		return true
	}}
	False := AdHoc{func(ex exTup2) bool {
		return false
	}}
	var predTests = []struct {
		name string
		in   Predicate
		out  bool
	}{
		{"True.EvalFunc(rexTup2)(tup)", True, true},
		{"False.EvalFunc(rexTup2)(tup)", False, false},

		{"Not(True).EvalFunc(rexTup2)(tup)", Not(True), false},
		{"Not(False).EvalFunc(rexTup2)(tup)", Not(False), true},

		{"True.And(True).EvalFunc(rexTup2)(tup)", True.And(True), true},
		{"False.And(True).EvalFunc(rexTup2)(tup)", False.And(True), false},
		{"True.And(False).EvalFunc(rexTup2)(tup)", True.And(False), false},
		{"False.And(False).EvalFunc(rexTup2)(tup)", False.And(False), false},

		{"True.Or(True).EvalFunc(rexTup2)(tup)", True.Or(True), true},
		{"False.Or(True).EvalFunc(rexTup2)(tup)", False.Or(True), true},
		{"True.Or(False).EvalFunc(rexTup2)(tup)", True.Or(False), true},
		{"False.Or(False).EvalFunc(rexTup2)(tup)", False.Or(False), false},

		{"True.Xor(True).EvalFunc(rexTup2)(tup)", True.Xor(True), false},
		{"False.Xor(True).EvalFunc(rexTup2)(tup)", False.Xor(True), true},
		{"True.Xor(False).EvalFunc(rexTup2)(tup)", True.Xor(False), true},
		{"False.Xor(False).EvalFunc(rexTup2)(tup)", False.Xor(False), false},
	}

	rexTup2 := reflect.TypeOf(exTup2{})
	tup := exTup2{1, "foo"}

	for _, tt := range predTests {
		b := tt.in.EvalFunc(rexTup2)(tup)
		if b != tt.out {
			t.Errorf("%s => %v, want %v", tt.name, b, tt.out)
		}
	}
}

func TestEQ(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, true},
		{exTupInt{1, 2}, false},
		{exTupString{"foo", "foo"}, true},
		{exTupString{"foo", "bar"}, false},
	}
	p := Foo.EQ(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v equals comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestEQLit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, true},
		{exTupInt{1}, 2, false},
		{exTupString{"foo"}, "foo", true},
		{exTupString{"foo"}, "bar", false},
	}
	for _, tt := range predTests {
		p := Foo.EQ(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal equals comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

func TestNE(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, false},
		{exTupInt{1, 2}, true},
		{exTupString{"foo", "foo"}, false},
		{exTupString{"foo", "bar"}, true},
	}
	p := Foo.NE(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v not equals comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestNELit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, false},
		{exTupInt{1}, 2, true},
		{exTupString{"foo"}, "foo", false},
		{exTupString{"foo"}, "bar", true},
	}
	for _, tt := range predTests {
		p := Foo.NE(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal not equals comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

func TestLT(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, false},
		{exTupInt{1, 2}, true},
		{exTupString{"foo", "foo"}, false},
		{exTupString{"bar", "foo"}, true},
	}
	p := Foo.LT(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v Less Than comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestLTLit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, false},
		{exTupInt{1}, 2, true},
		{exTupString{"foo"}, "foo", false},
		{exTupString{"bar"}, "foo", true},
	}
	for _, tt := range predTests {
		p := Foo.LT(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal Less Than comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

// LE

func TestLE(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, true},
		{exTupInt{1, 2}, true},
		{exTupString{"foo", "foo"}, true},
		{exTupString{"bar", "foo"}, true},
	}
	p := Foo.LE(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v Less Than or Equal to comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestLELit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, true},
		{exTupInt{1}, 2, true},
		{exTupString{"foo"}, "foo", true},
		{exTupString{"bar"}, "foo", true},
	}
	for _, tt := range predTests {
		p := Foo.LE(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal Less Than or Equal to comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

// GT

func TestGT(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, false},
		{exTupInt{1, 2}, false},
		{exTupString{"foo", "foo"}, false},
		{exTupString{"bar", "foo"}, false},
	}
	p := Foo.GT(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v Greater Than comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestGTLit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, false},
		{exTupInt{1}, 2, false},
		{exTupString{"foo"}, "foo", false},
		{exTupString{"bar"}, "foo", false},
	}
	for _, tt := range predTests {
		p := Foo.GT(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal Greater Than comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

// GE

func TestGE(t *testing.T) {
	type exTupInt struct {
		Foo int
		Bar int
	}
	type exTupString struct {
		Foo string
		Bar string
	}
	Foo := Attribute("Foo")
	Bar := Attribute("Bar")

	var predTests = []struct {
		in  interface{}
		out bool
	}{
		{exTupInt{1, 1}, true},
		{exTupInt{1, 2}, false},
		{exTupString{"foo", "foo"}, true},
		{exTupString{"bar", "foo"}, false},
	}
	p := Foo.GE(Bar).EvalFunc(reflect.TypeOf(exTupInt{}))
	for _, tt := range predTests {
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v greater than or equal to comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}
func TestGELit(t *testing.T) {
	type exTupInt struct {
		Foo int
	}
	type exTupString struct {
		Foo string
	}
	Foo := Attribute("Foo")

	var predTests = []struct {
		in  interface{}
		lit interface{}
		out bool
	}{
		{exTupInt{1}, 1, true},
		{exTupInt{1}, 2, false},
		{exTupString{"foo"}, "foo", true},
		{exTupString{"bar"}, "foo", false},
	}
	for _, tt := range predTests {
		p := Foo.GE(tt.lit).EvalFunc(reflect.TypeOf(exTupInt{}))
		b := p(tt.in)
		if b != tt.out {
			t.Errorf("%v literal greater than or equal to comparison => %v, want %v", tt.in, b, tt.out)
		}
	}
}

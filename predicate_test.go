package rel

import (
	"reflect"
	"testing"
)

// tests & benchmarks for Predicates

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

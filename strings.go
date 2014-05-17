// strings deals with string representation of relations

package rel

import (
	"bytes"
	"fmt"
	"reflect"
	"text/tabwriter"
)

// goStringTabTable is makes a gostring out of a given relation
// this isn't a method of relation (and then named GoString()) because
// go doesn't allow methods to be defined on interfaces.
func goStringTabTable(r Relation) string {
	// use a buffer to write to and later turn into a string
	s := bytes.NewBufferString("rel.New([]struct {\n")

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape)

	// create struct slice type information
	// TODO(jonlawlor): include tags?
	for _, att := range Heading(r) {
		fmt.Fprintf(w, "\t\xff%s\xff\t\xff%v\xff\t\n", att.Name, att.Type)
	}
	w.Flush()
	s.WriteString("}{\n")

	// write the body
	//TODO(jonlawlor): see if buffering the channel improves performance
	tups := make(chan T)
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := Deg(r)
	for tup := range tups {
		rtup := reflect.ValueOf(tup)
		// this part might be replacable with some workers that
		// convert tuples to strings
		fmt.Fprintf(w, "\t{")
		for j := 0; j < deg; j++ {
			f := rtup.Field(j)
			switch f.Kind() {
			case reflect.String:
				fmt.Fprintf(w, "\xff%q\xff,\t", f)
			case reflect.Bool:
				fmt.Fprintf(w, "%t,\t", f.Bool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// TODO(jonlawlor): I'm not sure all have to be enumerated
				fmt.Fprintf(w, "%d,\t", f.Int())
			case reflect.Float32, reflect.Float64:
				// TODO(jonlawlor): is there a general float type to use here?
				fmt.Fprintf(w, "%g,\t", f.Float())
			default:
				fmt.Fprintf(w, "\xff%v\xff,\t", f)
			}
		}
		fmt.Fprintf(w, "},\n")
	}

	w.Flush()
	s.WriteString("})")
	return s.String()
}

// stringTabTable is makes a gostring out of a given relation
// this isn't a method of relation (and then named GoString()) because
// go doesn't allow methods to be defined on interfaces.
func stringTabTable(r Relation) string {

	// use a buffer to write to and later turn into a string
	s := new(bytes.Buffer)

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	// align elements to the right as well
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape|tabwriter.AlignRight)

	//TODO(jonlawlor): not sure how to create the vertical seps like:
	//+---------+---------+---------+
	// which should go in between each of the sections of heading and body
	// also, I don't know where the candidate keys should go.  Date
	// does an underline but they can be overlapping, and I am not sure
	// that unicode allows arbitrary nesting of underlines?  In any case
	// it is not possible to arrange arbitrary candidate keys to be
	// adjacent.

	// create heading information
	for _, att := range Heading(r) {
		fmt.Fprintf(w, "\t\xff%s\xff\t\xff%v\xff\t\n", att.Name, att.Type)
	}

	// write the body
	//TODO(jonlawlor): see if buffering the channel improves performance
	tups := make(chan T)
	r.Tuples(tups)

	// TODO(jonlawlor): abstract the per-tuple functional mapping to another
	// method?
	deg := Deg(r)
	for tup := range tups {
		rtup := reflect.ValueOf(tup)
		// this part might be replacable with some workers that
		// convert tuples to strings
		for j := 0; j < deg; j++ {
			f := rtup.Field(j)
			switch f.Kind() {
			case reflect.String:
				fmt.Fprintf(w, "|\t \xff%s\xff ", f)
			case reflect.Bool:
				fmt.Fprintf(w, "|\t %t ", f.Bool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				// TODO(jonlawlor): I'm not sure all have to be enumerated
				fmt.Fprintf(w, "|\t %d ", f.Int())
			case reflect.Float32, reflect.Float64:
				// TODO(jonlawlor): there may be another representation
				fmt.Fprintf(w, "|\t %g ", f.Float())
			default:
				fmt.Fprintf(w, "|\t \xff%v\xff ", f)
			}
		}
		fmt.Fprintf(w, "\t|\n")
	}

	w.Flush()
	return s.String()
}

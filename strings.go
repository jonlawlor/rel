// strings deals with string representation of relations

package rel

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"text/tabwriter"
)

// really not sure about these

// goStringTabTable is makes a gostring out of a given relation
func goStringTabTable(r Relation) string {
	// use a buffer to write to and later turn into a string
	s := bytes.NewBufferString("rel.New([]struct {\n")

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape)

	// create struct slice type information
	// TODO(jonlawlor): include tags?
	cn := Heading(r)
	ct := FieldTypes(reflect.TypeOf(r.Zero()))
	for i := range cn {
		fmt.Fprintf(w, "\t\xff%s\xff\t\xff%v\xff\t\n", cn[i], ct[i])
	}
	w.Flush()
	s.WriteString("}{\n")

	// write the body
	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(r.Zero())), 0)
	tups := body.Interface()
	_ = r.TupleChan(tups)

	deg := Deg(r)
	sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
	inCases := []reflect.SelectCase{sourceSel}

	for {
		_, rtup, ok := reflect.Select(inCases)
		if !ok {
			break
		}
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

// PrettyPrint returns a human readable table of the tuples in the relation.
func PrettyPrint(r Relation) string {
	return stringTabTable(r)
}

// stringTabTable is makes a gostring out of a given relation
func stringTabTable(r Relation) string {

	// use a buffer to write to and later turn into a string
	s := new(bytes.Buffer)

	w := new(tabwriter.Writer)
	// \xff is used as an escape delim; see the tabwriter docs
	// align elements to the right as well
	w.Init(s, 1, 1, 1, ' ', tabwriter.StripEscape|tabwriter.AlignRight)

	// create heading information
	deg := Deg(r)

	// make a spacer, to be replaced later
	for i := 0; i < deg; i++ {
		fmt.Fprintf(w, "+\t ")
	}
	fmt.Fprintf(w, "\t+\n")

	// heading
	cn := Heading(r)
	for _, name := range cn {
		fmt.Fprintf(w, "|\t \xff%s\xff ", name)
	}
	fmt.Fprintf(w, "\t|\n")

	// write the body
	body := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(r.Zero())), 0)
	tups := body.Interface()
	_ = r.TupleChan(tups)

	sourceSel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: body}
	inCases := []reflect.SelectCase{sourceSel}

	for {
		_, rtup, ok := reflect.Select(inCases)
		if !ok {
			break
		}
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
				fmt.Fprintf(w, "|\t %d ", f.Int())
			case reflect.Float32, reflect.Float64:
				fmt.Fprintf(w, "|\t %g ", f.Float())
			default:
				fmt.Fprintf(w, "|\t \xff%v\xff ", f)
			}
		}
		fmt.Fprintf(w, "\t|\n")
	}

	w.Flush()
	str := s.String()

	// replace the blanks in the spacers with "-"
	// TODO(jonlawlor): maybe there is a way to do this during construction
	// instead of afterwards?

	lineWidth := strings.Index(str, "\n")
	sep := " " + strings.Replace(str[1:lineWidth], " ", "-", -1)
	return sep + str[lineWidth:lineWidth*2+2] + sep + str[lineWidth*2+1:] + sep
}

// union implements a union expression in relational algebra

package rel

func (r1 *Simple) UnionExpr(r2 *Relation) {
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
	}()

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

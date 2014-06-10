package rel

/*  for future use in a relation Copy method
import "sync"

// TODO(jonlawlor) incorporate this in functions such as join, union, and
// setdiff which may require re-reading the data from a channel.

type request struct {
	i   int    // index of the element requested
	got chan T // channel to respond on
}

type Memory struct {
	in  chan T
	req chan request
	sync.RWMutex
	memory []T
}

func Replay(in chan T) Memory {
	m := Memory{in: in, req: make(chan request)}
	go m.Feed()
	return m
}

func (m *Memory) Feed() {
	for req := range m.req {
		// req.i can at most be one higher than the current max index
		if req.i <= len(m.memory)-1 {
			req.got <- m.memory[req.i]
			continue
		}
		v, ok := <-m.in
		if !ok {
			close(req.got)
			continue
		}
		m.Lock()
		m.memory = append(m.memory, v)
		m.Unlock()
		req.got <- v
	}
}

func (m *Memory) Copy(to chan T) {
	got := make(chan T)
	var i int

	i = m.copyFromMem(i, to)
	for {
		// if a different copy gets ahead of this one and then blocks, it is
		// possible that this request will block until the other request
		// returns, even though this request may be asking for an element that
		// is before the end of the memory.
		// TODO(jonlawlor): fix this by repeating the copyFromMem function? We
		// might have to return something more in the response.
		m.req <- request{i, got}
		resp, ok := <-got
		if !ok {
			close(to)
			return
		}
		to <- resp
		i++
	}
}

func (m *Memory) copyFromMem(i int, to chan T) int {
	// maybe this should be cancellable?
	m.RLock()
	if len(m.memory) == 0 {
		m.RUnlock()
		return i
	}
	mem := m.memory[i:]
	m.RUnlock()
	for _, v := range mem {
		to <- v
		i++
	}
	return i
}
*/

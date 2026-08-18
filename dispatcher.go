package actioncable

import "sync"

// A dispatcher runs a subscription's callbacks on their own goroutine, one at a
// time, in the order the events happened.
//
// Callbacks belong off the connection's goroutine: OnDisconnected calling Close
// or OnConnected calling Subscribe are both reasonable things to write, and both
// wait on work only the connection goroutine can do. The queue is unbounded for
// the same reason — handing an event over must never block the connection.
type dispatcher struct {
	mu      sync.Mutex
	pending []func()
	awake   chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newDispatcher() *dispatcher {
	dispatcher := &dispatcher{
		awake: make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
	go dispatcher.run()

	return dispatcher
}

func (d *dispatcher) dispatch(callback func()) {
	if callback == nil {
		return
	}

	d.mu.Lock()
	d.pending = append(d.pending, callback)
	d.mu.Unlock()

	select {
	case d.awake <- struct{}{}:
	default:
	}
}

// stop lets the dispatcher finish what it has and go away. It doesn't wait,
// since a callback is allowed to be what stopped it.
func (d *dispatcher) stop() {
	d.once.Do(func() { close(d.done) })
}

func (d *dispatcher) run() {
	for {
		select {
		case <-d.awake:
			d.drain()
		case <-d.done:
			d.drain()
			return
		}
	}
}

func (d *dispatcher) drain() {
	for {
		d.mu.Lock()
		if len(d.pending) == 0 {
			d.mu.Unlock()
			return
		}
		callback := d.pending[0]
		d.pending = d.pending[1:]
		d.mu.Unlock()

		callback()
	}
}

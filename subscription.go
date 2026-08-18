package actioncable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// A Subscription is one channel subscription on a client. Read what the channel
// sends from Messages, and talk back with Perform or Send.
type Subscription struct {
	client     *Client
	identifier string

	confirmed chan struct{}
	rejected  chan struct{}
	pending   atomic.Bool

	callbacks      *dispatcher
	onConnected    func(reconnected bool)
	onDisconnected func(willReconnect bool)
	onRejected     func()

	// sendMu guards the messages channel, which anyone may close by
	// unsubscribing while the connection is delivering to it.
	sendMu   sync.Mutex
	messages chan Message
	closed   bool

	confirmOnce sync.Once
	rejectOnce  sync.Once
}

func newSubscription(client *Client, identifier string, buffer int, options []SubscriptionOption) *Subscription {
	subscription := &Subscription{
		client:     client,
		identifier: identifier,
		messages:   make(chan Message, buffer),
		confirmed:  make(chan struct{}),
		rejected:   make(chan struct{}),
		callbacks:  newDispatcher(),
	}
	subscription.pending.Store(true)

	for _, option := range options {
		option(subscription)
	}

	return subscription
}

// Key is the JSON identifier string the server knows this subscription by, and
// the one it echoes back on everything it sends here.
func (s *Subscription) Key() string {
	return s.identifier
}

// Messages carries everything the channel broadcasts or transmits to this
// subscription. It closes when the subscription is unsubscribed or the client is
// closed.
//
// Read it promptly. Messages that arrive with the buffer full are dropped and
// logged rather than stalling the connection — WithMessageBuffer sizes the buffer
// for a slow consumer.
func (s *Subscription) Messages() <-chan Message {
	return s.messages
}

// Perform invokes an action on the channel — the equivalent of the JavaScript
// client's perform. data must encode to a JSON object, and may be nil.
func (s *Subscription) Perform(ctx context.Context, action string, data any) error {
	payload, err := performPayload(action, data)
	if err != nil {
		return err
	}

	return s.client.send(ctx, Command{Name: CommandMessage, Identifier: s.identifier, Data: string(payload)})
}

// Send delivers data to the channel as-is, without naming an action. Rails
// routes it to the channel's receive method.
func (s *Subscription) Send(ctx context.Context, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("actioncable: encoding data for %s: %w", s.identifier, err)
	}

	return s.client.send(ctx, Command{Name: CommandMessage, Identifier: s.identifier, Data: string(payload)})
}

// Unsubscribe tells the server to drop the subscription and closes Messages.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	if last := s.client.forget(s); last {
		return s.client.send(ctx, Command{Name: CommandUnsubscribe, Identifier: s.identifier})
	} else {
		return nil
	}
}

func (s *Subscription) confirm(reconnected bool) {
	// Only a subscription that was waiting to be confirmed has news. The server
	// can confirm twice when a retried subscribe crosses the first confirmation.
	if !s.pending.Swap(false) {
		return
	}
	s.confirmOnce.Do(func() { close(s.confirmed) })

	if s.onConnected != nil {
		s.callbacks.dispatch(func() { s.onConnected(reconnected) })
	}
}

func (s *Subscription) reject() {
	s.pending.Store(false)
	s.rejectOnce.Do(func() { close(s.rejected) })

	if s.onRejected != nil {
		s.callbacks.dispatch(s.onRejected)
	}
	s.close()
}

func (s *Subscription) disconnect(willReconnect bool) {
	s.pending.Store(false)

	if s.onDisconnected != nil {
		s.callbacks.dispatch(func() { s.onDisconnected(willReconnect) })
	}
}

func (s *Subscription) deliver(message Message) bool {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	// A closed subscription has nothing left to receive, and nothing to report.
	if s.closed {
		return true
	}

	select {
	case s.messages <- message:
		return true
	default:
		return false
	}
}

func (s *Subscription) close() {
	s.sendMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.messages)
	}
	s.sendMu.Unlock()

	s.callbacks.stop()
}

func performPayload(action string, data any) ([]byte, error) {
	fields := map[string]json.RawMessage{}

	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("actioncable: encoding data for %q: %w", action, err)
		}
		if err := json.Unmarshal(encoded, &fields); err != nil {
			return nil, fmt.Errorf("actioncable: data for %q must encode to a JSON object: %w", action, err)
		}
	}

	name, err := json.Marshal(action)
	if err != nil {
		return nil, fmt.Errorf("actioncable: encoding action %q: %w", action, err)
	}
	fields["action"] = name

	return json.Marshal(fields)
}

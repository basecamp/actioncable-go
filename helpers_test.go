package actioncable

import (
	"context"
	"sync"
	"testing"
	"time"
)

// wait is how long a test will hang around for something that should already
// have happened.
const wait = 2 * time.Second

func newTestClient(t *testing.T, transport Transport, options ...Option) *Client {
	t.Helper()

	client := New("ws://cable.example.com/cable", append([]Option{
		WithTransport(transport),
		WithLogger(testLogger(t)),
	}, options...)...)
	t.Cleanup(func() { client.Close() })

	return client
}

func room() Identifier {
	return Identifier{Channel: "RoomChannel", Params: Params{"id": 42}}
}

func connect(client *Client) <-chan error {
	connecting := make(chan error, 1)
	go func() { connecting <- client.Connect(context.Background()) }()

	return connecting
}

// welcomed connects a client and plays the server's welcome, returning the
// connection the test can go on talking over.
func welcomed(t *testing.T, client *Client, transport *fakeTransport) *fakeConn {
	t.Helper()

	connecting := connect(client)
	conn := transport.accept(t)
	conn.welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	return conn
}

type subscribeResult struct {
	subscription *Subscription
	err          error
}

// subscribe subscribes in the background, since Subscribe waits for the
// confirmation the test still has to send.
func subscribe(client *Client, identifier Identifier, options ...SubscriptionOption) <-chan subscribeResult {
	subscribing := make(chan subscribeResult, 1)
	go func() {
		subscription, err := client.Subscribe(context.Background(), identifier, options...)
		subscribing <- subscribeResult{subscription, err}
	}()

	return subscribing
}

func subscribed(t *testing.T, client *Client, conn *fakeConn) *Subscription {
	t.Helper()

	subscribing := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)

	result := <-subscribing
	if result.err != nil {
		t.Fatalf("Subscribe: %v", result.err)
	}

	return result.subscription
}

func receive(t *testing.T, subscription *Subscription) Message {
	t.Helper()

	select {
	case message, open := <-subscription.Messages():
		if !open {
			t.Fatal("messages channel closed")
		}
		return message
	case <-time.After(wait):
		t.Fatal("no message arrived")
		return nil
	}
}

// testLogger sends the client's chatter to the test log, and falls silent once
// the test is over so a stray line can never reach a finished test.
func testLogger(tb testing.TB) Logger {
	var mu sync.Mutex
	over := false
	tb.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		over = true
	})

	return LoggerFunc(func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()

		if !over {
			tb.Logf(format, args...)
		}
	})
}

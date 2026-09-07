package actioncable

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeTransport hands out in-memory connections a test can play the server on.
type fakeTransport struct {
	subprotocol string
	dialed      chan *fakeConn

	mu       sync.Mutex
	dialErrs []error
	options  DialOptions
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{subprotocol: SubprotocolV1JSON, dialed: make(chan *fakeConn, 16)}
}

func (t *fakeTransport) Dial(ctx context.Context, url string, options DialOptions) (Conn, error) {
	t.mu.Lock()
	t.options = options
	if len(t.dialErrs) > 0 {
		err := t.dialErrs[0]
		t.dialErrs = t.dialErrs[1:]
		t.mu.Unlock()
		return nil, err
	}
	t.mu.Unlock()

	conn := &fakeConn{
		subprotocol: t.subprotocol,
		incoming:    make(chan []byte),
		outgoing:    make(chan []byte, 32),
		closed:      make(chan struct{}),
		subscribed:  map[string]bool{},
	}
	t.dialed <- conn

	return conn, nil
}

// dialedWith reports the options the client last dialed with.
func (t *fakeTransport) dialedWith() DialOptions {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.options
}

func (t *fakeTransport) failNextDial(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.dialErrs = append(t.dialErrs, err)
}

func (t *fakeTransport) accept(tb testing.TB) *fakeConn {
	tb.Helper()

	select {
	case conn := <-t.dialed:
		return conn
	case <-time.After(wait):
		tb.Fatal("no connection was dialed")
		return nil
	}
}

func (t *fakeTransport) refuseDial(tb testing.TB) {
	tb.Helper()

	select {
	case conn := <-t.dialed:
		tb.Fatalf("expected no connection, got one with subprotocol %q", conn.Subprotocol())
	case <-time.After(200 * time.Millisecond):
	}
}

// fakeConn is one connection with the test playing the server on the other end.
// Like Rails it keeps one subscription per identifier: a subscribe for an
// identifier it has already heard, answered or not, is ignored.
type fakeConn struct {
	subprotocol string
	incoming    chan []byte
	outgoing    chan []byte
	closed      chan struct{}
	closeOnce   sync.Once

	mu         sync.Mutex
	subscribed map[string]bool
}

func (c *fakeConn) Subprotocol() string {
	return c.subprotocol
}

func (c *fakeConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-c.incoming:
		return payload, nil
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) Write(ctx context.Context, payload []byte) error {
	if c.ignores(payload) {
		return nil
	}

	select {
	case c.outgoing <- payload:
		return nil
	case <-c.closed:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ignores reports whether the server would drop the command without a word: Rails
// does that to a second subscribe for an identifier the connection already has.
func (c *fakeConn) ignores(payload []byte) bool {
	var command v1JSONCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return CommandName(command.Command) == CommandSubscribe && c.subscribed[command.Identifier]
}

func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// push plays a server frame to the client.
func (c *fakeConn) push(tb testing.TB, frame string) {
	tb.Helper()

	select {
	case c.incoming <- []byte(frame):
	case <-c.closed:
		tb.Fatalf("connection closed before %s could be sent", frame)
	case <-time.After(wait):
		tb.Fatalf("client never read %s", frame)
	}
}

func (c *fakeConn) welcome(tb testing.TB) {
	tb.Helper()
	c.push(tb, `{"type":"welcome"}`)
}

func (c *fakeConn) confirm(tb testing.TB, identifier string) {
	tb.Helper()
	c.push(tb, `{"type":"confirm_subscription","identifier":`+quote(identifier)+`}`)
}

// reject turns a subscription down, which also forgets it: the client is free to
// try again.
func (c *fakeConn) reject(tb testing.TB, identifier string) {
	tb.Helper()

	c.mu.Lock()
	delete(c.subscribed, identifier)
	c.mu.Unlock()

	c.push(tb, `{"type":"reject_subscription","identifier":`+quote(identifier)+`}`)
}

// sent waits for the next payload the client writes, exactly as it went out.
func (c *fakeConn) sent(tb testing.TB) []byte {
	tb.Helper()

	select {
	case payload := <-c.outgoing:
		return payload
	case <-time.After(wait):
		tb.Fatal("client sent nothing")
		return nil
	}
}

// next waits for the next command the client sends. Nobody has heard it yet:
// command and dropCommand settle that.
func (c *fakeConn) next(tb testing.TB) v1JSONCommand {
	tb.Helper()

	payload := c.sent(tb)

	var command v1JSONCommand
	require.NoError(tb, json.Unmarshal(payload, &command), "decoding command %s", payload)

	return command
}

// command waits for the next command the client sends and takes it in the way
// the server would.
func (c *fakeConn) command(tb testing.TB) v1JSONCommand {
	tb.Helper()

	command := c.next(tb)
	c.hear(command)

	return command
}

func (c *fakeConn) hear(command v1JSONCommand) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch CommandName(command.Command) {
	case CommandSubscribe:
		c.subscribed[command.Identifier] = true
	case CommandUnsubscribe:
		delete(c.subscribed, command.Identifier)
	}
}

func (c *fakeConn) expectCommand(tb testing.TB, name CommandName, identifier string) v1JSONCommand {
	tb.Helper()

	command := c.command(tb)
	require.Equal(tb, string(name), command.Command)
	require.Equal(tb, identifier, command.Identifier)

	return command
}

// dropCommand lets the next command fall on the floor, the way the server drops
// a subscribe that reaches it before the connection is set up.
func (c *fakeConn) dropCommand(tb testing.TB, name CommandName, identifier string) {
	tb.Helper()

	command := c.next(tb)
	require.Equal(tb, string(name), command.Command)
	require.Equal(tb, identifier, command.Identifier)
}

// expectNoCommand fails when the client sends anything in the next little while.
func (c *fakeConn) expectNoCommand(tb testing.TB) {
	tb.Helper()

	select {
	case payload := <-c.outgoing:
		tb.Fatalf("expected no command, got %s", payload)
	case <-time.After(100 * time.Millisecond):
	}
}

func quote(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	return string(encoded)
}

// fakeProtocol speaks a made-up subprotocol and stamps everything it encodes, so
// a test can tell which protocol the client settled on.
type fakeProtocol struct {
	subprotocol string
	stamp       string
}

func (p fakeProtocol) Subprotocol() string {
	return p.subprotocol
}

func (p fakeProtocol) Encode(command Command) ([]byte, error) {
	payload, err := V1JSON{}.Encode(command)
	if err != nil {
		return nil, err
	}

	return append([]byte(p.stamp), payload...), nil
}

func (p fakeProtocol) Decode(payload []byte) (Incoming, error) {
	return V1JSON{}.Decode(bytes.TrimPrefix(payload, []byte(p.stamp)))
}

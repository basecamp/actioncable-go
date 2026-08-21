package actioncable

import (
	"context"
	"net/http"
	"slices"
	"time"
)

// An Option configures a client.
type Option func(*Client)

// WithTransport swaps the network handler. The default is WebSocketTransport.
func WithTransport(transport Transport) Option {
	return func(c *Client) { c.transport = transport }
}

// WithProtocols sets the protocols offered during the handshake, most preferred
// first, replacing the default of V1JSON. The server picks one of them and the
// client speaks it for the rest of the connection.
func WithProtocols(protocols ...Protocol) Option {
	return func(c *Client) { c.protocols = slices.Clone(protocols) }
}

// WithAdditionalProtocols offers protocols ahead of the ones already there, so
// preferring a new protocol doesn't mean restating the ones to fall back to.
func WithAdditionalProtocols(protocols ...Protocol) Option {
	return func(c *Client) { c.protocols = append(slices.Clone(protocols), c.protocols...) }
}

// WithHeader sets the headers sent on the upgrade request. An Action Cable server
// authorizes that request, so this is where a session cookie or a bearer token
// goes.
func WithHeader(header http.Header) Option {
	return func(c *Client) { c.header = header.Clone() }
}

// WithHeaderFunc sets the headers the same way WithHeader does, except that it is
// asked on every dial rather than once. A client reconnects on its own for as long
// as it runs, which is longer than a credential that expires lives, and a reconnect
// carrying the token the first dial used would be turned down for good. What this
// returns is laid over the headers already set, so an Origin or a token given with
// WithHeader survives.
//
// An error turns down that dial, and the client tries again on its backoff.
func WithHeaderFunc(build func(ctx context.Context) (http.Header, error)) Option {
	return func(c *Client) { c.headerFunc = build }
}

// WithCookie is shorthand for sending one Cookie header.
func WithCookie(cookie string) Option {
	return func(c *Client) { c.setHeader("Cookie", cookie) }
}

// WithOrigin sets the Origin header. Rails checks it unless the server disables
// request forgery protection.
func WithOrigin(origin string) Option {
	return func(c *Client) { c.setHeader("Origin", origin) }
}

func (c *Client) setHeader(name, value string) {
	if c.header == nil {
		c.header = http.Header{}
	}
	c.header.Set(name, value)
}

// WithLogger sends the client's chatter — dropped messages, failed connections,
// retries — somewhere. Nothing is logged by default.
func WithLogger(logger Logger) Option {
	return func(c *Client) { c.logger = logger }
}

// WithStaleAfter sets how long a connection may go without a frame before it
// counts as dead. The server beats every three seconds; the default is six, so
// two missed beats.
func WithStaleAfter(after time.Duration) Option {
	return func(c *Client) { c.staleAfter = after }
}

// WithBackoff sets the reconnect delay. It starts at initial, doubles per failed
// attempt up to longest, and is spread with jitter. Defaults to a second and
// half a minute.
func WithBackoff(initial, longest time.Duration) Option {
	return func(c *Client) {
		c.initialBackoff = initial
		c.longestBackoff = longest
	}
}

// WithSubscribeRetry sets how often an unconfirmed subscribe command is resent.
// Defaults to half a second, like the JavaScript client's guarantor.
func WithSubscribeRetry(retry time.Duration) Option {
	return func(c *Client) { c.subscribeRetry = retry }
}

// WithMessageBuffer sets how many messages a subscription buffers before it
// starts dropping them. Defaults to 64.
func WithMessageBuffer(messages int) Option {
	return func(c *Client) { c.messageBuffer = messages }
}

// A SubscriptionOption configures a subscription. Callbacks run on their own
// goroutine, one at a time, in the order the events happened, so Close,
// Subscribe, and Unsubscribe all work from inside one.
type SubscriptionOption func(*Subscription)

// OnConnected is called every time the server confirms the subscription,
// including after a reconnect — which is what reconnected reports.
func OnConnected(callback func(reconnected bool)) SubscriptionOption {
	return func(s *Subscription) { s.onConnected = callback }
}

// OnDisconnected is called when the connection drops, with whether the client
// intends to dial again.
func OnDisconnected(callback func(willReconnect bool)) SubscriptionOption {
	return func(s *Subscription) { s.onDisconnected = callback }
}

// OnRejected is called when the channel rejects the subscription.
func OnRejected(callback func()) SubscriptionOption {
	return func(s *Subscription) { s.onRejected = callback }
}

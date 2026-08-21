package actioncable

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// A Client owns one connection to an Action Cable server and the subscriptions
// running over it. Create one with New, start it with Connect, and hang up with
// Close. It is safe for concurrent use.
type Client struct {
	url        string
	transport  Transport
	protocols  []Protocol
	header     http.Header
	headerFunc func(ctx context.Context) (http.Header, error)
	logger     Logger

	staleAfter     time.Duration
	subscribeRetry time.Duration
	initialBackoff time.Duration
	longestBackoff time.Duration
	messageBuffer  int

	// mu guards every field below it.
	mu   sync.Mutex
	conn Conn
	// protocol is the one the server picked for the connection in hand.
	protocol      Protocol
	subscriptions map[string][]*Subscription
	attempts      int
	reconnected   bool
	welcomed      bool
	everWelcomed  bool
	stopped       bool
	failure       error
	cancel        context.CancelFunc

	// writeMu serializes writes to the connection. It is its own lock so a slow
	// write doesn't hold up everything else reading the client's state.
	writeMu sync.Mutex

	connected     chan struct{}
	connectedOnce sync.Once
	done          chan struct{}
	doneOnce      sync.Once
}

// New builds a client for an Action Cable endpoint, typically wss://host/cable.
// It does not touch the network until Connect.
func New(url string, options ...Option) *Client {
	client := &Client{
		url:            url,
		transport:      &WebSocketTransport{},
		protocols:      []Protocol{V1JSON{}},
		logger:         discardLogger{},
		staleAfter:     6 * time.Second,
		subscribeRetry: 500 * time.Millisecond,
		initialBackoff: time.Second,
		longestBackoff: 30 * time.Second,
		messageBuffer:  64,
		subscriptions:  map[string][]*Subscription{},
		connected:      make(chan struct{}),
		done:           make(chan struct{}),
	}

	for _, option := range options {
		option(client)
	}

	client.assumeOrigin()

	return client
}

// assumeOrigin fills in an Origin for the opening request when none was given.
// Rails compares Origin against the host it serves on and turns down anything
// else, a request carrying no Origin at all included, so the Action Cable URL's
// own origin is the one that gets in. A server behind a proxy that terminates TLS
// sees a different scheme than the URL says, and needs WithOrigin to say so.
func (c *Client) assumeOrigin() {
	if c.header.Get("Origin") != "" {
		return
	}

	if origin := originOf(c.url); origin != "" {
		c.setHeader("Origin", origin)
	}
}

// dialHeader is what the opening request carries. Without WithHeaderFunc that is
// what was set once, at construction; with it, what the caller says now, laid over
// the headers already there.
func (c *Client) dialHeader(ctx context.Context) (http.Header, error) {
	if c.headerFunc == nil {
		return c.header, nil
	}

	current, err := c.headerFunc(ctx)
	if err != nil {
		return nil, err
	}

	header := c.header.Clone()
	if header == nil {
		header = http.Header{}
	}
	for name, values := range current {
		header[name] = values
	}

	return header, nil
}

func originOf(rawURL string) string {
	endpoint, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	switch strings.ToLower(endpoint.Scheme) {
	case "wss", "https":
		return "https://" + endpoint.Host
	case "ws", "http":
		return "http://" + endpoint.Host
	default:
		return ""
	}
}

// Connect starts the client and returns once the server has sent its welcome.
// Failed connection attempts are retried until that happens, ctx is done, or
// the server tells us not to come back.
//
// ctx bounds the wait, not the client: the connection lives until Close.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		failure := c.failureLocked()
		c.mu.Unlock()
		return failure
	}
	if c.cancel != nil {
		c.mu.Unlock()
		return ErrAlreadyConnected
	}
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel
	c.mu.Unlock()

	go c.run(runContext)

	select {
	case <-c.connected:
		return nil
	case <-c.done:
		return c.stoppedBecause()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Connected reports whether a connection is up and welcomed.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.welcomed && c.conn != nil
}

// Subscribe subscribes to a channel and returns once the server confirms it.
// The subscription outlives reconnects — it is resubscribed automatically — so it
// stays valid until Unsubscribe.
//
// It returns ErrRejected when the channel turns the subscription down.
func (c *Client) Subscribe(ctx context.Context, identifier Identifier, options ...SubscriptionOption) (*Subscription, error) {
	key, err := identifier.key()
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.stopped {
		failure := c.failureLocked()
		c.mu.Unlock()
		return nil, failure
	}
	if c.cancel == nil {
		c.mu.Unlock()
		return nil, ErrNotConnected
	}
	subscription := newSubscription(c, key, c.messageBuffer, options)
	c.subscriptions[key] = append(c.subscriptions[key], subscription)
	c.mu.Unlock()

	if err := c.send(ctx, Command{Name: CommandSubscribe, Identifier: key}); err != nil {
		// Nothing to do about it here: the connection will subscribe again as
		// soon as it is welcomed back.
		c.logger.Printf("actioncable: subscribing to %s: %v", key, err)
	}

	select {
	case <-subscription.confirmed:
		return subscription, nil
	case <-subscription.rejected:
		c.forget(subscription)
		return nil, fmt.Errorf("%w: %s", ErrRejected, key)
	case <-c.done:
		c.forget(subscription)
		return nil, c.stoppedBecause()
	case <-ctx.Done():
		c.forget(subscription)
		return nil, ctx.Err()
	}
}

// Close hangs up, stops reconnecting, and closes every subscription's message
// channel. It is safe to call from a subscription callback, and safe to call
// twice.
func (c *Client) Close() error {
	c.mu.Lock()
	cancel, conn := c.cancel, c.conn
	c.stopped = true
	if c.failure == nil {
		c.failure = ErrClosed
	}
	c.mu.Unlock()

	if cancel == nil {
		// Nothing was ever started, so nothing will finish it for us.
		c.finish()
		return nil
	}

	cancel()
	if conn != nil {
		conn.Close()
	}
	<-c.done

	return nil
}

func (c *Client) run(ctx context.Context) {
	defer c.finish()
	defer c.closeSubscriptions()

	for {
		if err := c.session(ctx); err != nil && !c.isStopped() {
			c.logger.Printf("actioncable: connection to %s ended: %v", c.url, err)
		}

		if c.isStopped() || ctx.Err() != nil {
			return
		}

		c.countAttempt()

		select {
		case <-ctx.Done():
			return
		case <-time.After(c.reconnectDelay()):
		}
	}
}

// session runs one connection from dial to hangup, and returns why it ended.
func (c *Client) session(ctx context.Context) error {
	if len(c.protocols) == 0 {
		return c.stop(ErrNoProtocols)
	}

	header, err := c.dialHeader(ctx)
	if err != nil {
		return err
	}

	conn, err := c.transport.Dial(ctx, c.url, DialOptions{
		Subprotocols: append(c.subprotocols(), SubprotocolUnsupported),
		Header:       header,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	protocol, err := c.negotiated(conn.Subprotocol())
	if err != nil {
		return c.stop(err)
	}

	c.mu.Lock()
	c.conn, c.protocol = conn, protocol
	c.mu.Unlock()
	defer c.disconnect()

	guaranteeing, stopGuaranteeing := context.WithCancel(ctx)
	guaranteed := make(chan struct{})
	go func() {
		defer close(guaranteed)
		c.guaranteeSubscriptions(guaranteeing)
	}()
	defer func() {
		stopGuaranteeing()
		<-guaranteed
	}()

	return c.receive(ctx, conn, protocol)
}

// subprotocols names every protocol the client can speak, most preferred first.
func (c *Client) subprotocols() []string {
	names := make([]string, 0, len(c.protocols)+1)
	for _, protocol := range c.protocols {
		names = append(names, protocol.Subprotocol())
	}

	return names
}

// negotiated finds the protocol the server picked out of the ones offered. A
// server that picks the sentinel, names something never offered, or names
// nothing at all leaves nothing to talk over, and dialing again won't change it.
func (c *Client) negotiated(subprotocol string) (Protocol, error) {
	for _, protocol := range c.protocols {
		if protocol.Subprotocol() == subprotocol {
			return protocol, nil
		}
	}

	if subprotocol == SubprotocolUnsupported {
		return nil, fmt.Errorf("%w: the server speaks none of %s", ErrUnsupportedSubprotocol, strings.Join(c.subprotocols(), ", "))
	} else {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedSubprotocol, subprotocol)
	}
}

// receive reads until the connection dies. A connection that has gone quiet for
// longer than staleAfter is dead: the server beats a ping every three seconds.
func (c *Client) receive(ctx context.Context, conn Conn, protocol Protocol) error {
	for {
		reading, cancelRead := context.WithTimeout(ctx, c.staleAfter)
		payload, err := conn.Read(reading)
		cancelRead()

		if err != nil {
			// A deadline the caller's context didn't cause is our own staleness
			// timeout rather than a cancellation.
			if errors.Is(reading.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return fmt.Errorf("actioncable: no frame in %s: %w", c.staleAfter, err)
			} else {
				return err
			}
		}

		if err := c.dispatch(ctx, protocol, payload); err != nil {
			return err
		}
	}
}

func (c *Client) dispatch(ctx context.Context, protocol Protocol, payload []byte) error {
	incoming, err := protocol.Decode(payload)
	if err != nil {
		c.logger.Printf("actioncable: dropping undecodable frame: %v", err)
		return nil
	}

	switch incoming.Kind {
	case KindWelcome:
		c.welcome(ctx)
	case KindPing:
		// The frame itself is the heartbeat, and reading it already reset the
		// staleness deadline.
	case KindDisconnect:
		return c.hangUp(incoming)
	case KindConfirmation:
		c.confirm(incoming.Identifier)
	case KindRejection:
		c.reject(incoming.Identifier)
	case KindMessage:
		c.deliver(incoming)
	}

	return nil
}

// welcome resets the connection's health and resubscribes everything, the way
// the server expects after every fresh connection.
func (c *Client) welcome(ctx context.Context) {
	c.mu.Lock()
	c.attempts = 0
	c.welcomed = true
	c.reconnected = c.everWelcomed
	c.everWelcomed = true
	subscriptions := c.allSubscriptionsLocked()
	c.mu.Unlock()

	c.connectedOnce.Do(func() { close(c.connected) })

	for _, subscription := range subscriptions {
		subscription.pending.Store(true)
		if err := c.send(ctx, Command{Name: CommandSubscribe, Identifier: subscription.identifier}); err != nil {
			c.logger.Printf("actioncable: resubscribing to %s: %v", subscription.identifier, err)
		}
	}
}

// guaranteeSubscriptions resends subscribe commands until they are confirmed. A
// subscribe sent while the server was still setting the connection up is simply
// dropped on the floor, so unconfirmed means unheard.
func (c *Client) guaranteeSubscriptions(ctx context.Context) {
	ticker := time.NewTicker(c.subscribeRetry)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, subscription := range c.pendingSubscriptions() {
				if err := c.send(ctx, Command{Name: CommandSubscribe, Identifier: subscription.identifier}); err != nil {
					c.logger.Printf("actioncable: resubscribing to %s: %v", subscription.identifier, err)
				}
			}
		}
	}
}

func (c *Client) confirm(identifier string) {
	c.mu.Lock()
	subscriptions, reconnected := c.subscriptions[identifier], c.reconnected
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.confirm(reconnected)
	}
}

func (c *Client) reject(identifier string) {
	c.mu.Lock()
	subscriptions := c.subscriptions[identifier]
	delete(c.subscriptions, identifier)
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.reject()
	}
}

func (c *Client) deliver(incoming Incoming) {
	c.mu.Lock()
	subscriptions := c.subscriptions[incoming.Identifier]
	c.mu.Unlock()

	if len(subscriptions) == 0 {
		c.logger.Printf("actioncable: no subscription for %s, dropping message", incoming.Identifier)
		return
	}

	for _, subscription := range subscriptions {
		if !subscription.deliver(incoming.Message) {
			c.logger.Printf("actioncable: message buffer full for %s, dropping message", incoming.Identifier)
		}
	}
}

func (c *Client) hangUp(incoming Incoming) error {
	disconnect := &DisconnectError{Reason: incoming.Reason, Reconnect: incoming.Reconnect}
	if incoming.Reconnect {
		return disconnect
	} else {
		return c.stop(disconnect)
	}
}

// disconnect tears down the current connection and tells every subscription.
func (c *Client) disconnect() {
	c.mu.Lock()
	c.conn = nil
	c.protocol = nil
	c.welcomed = false
	subscriptions := c.allSubscriptionsLocked()
	willReconnect := !c.stopped
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.disconnect(willReconnect)
	}
}

func (c *Client) send(ctx context.Context, command Command) error {
	c.mu.Lock()
	conn, protocol, welcomed := c.conn, c.protocol, c.welcomed
	c.mu.Unlock()

	// Before the welcome the server hasn't finished setting the connection up
	// and throws away whatever it receives, so there is nowhere to send yet.
	if conn == nil || !welcomed {
		return ErrNotConnected
	}

	payload, err := protocol.Encode(command)
	if err != nil {
		return fmt.Errorf("actioncable: encoding %s command: %w", command.Name, err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return conn.Write(ctx, payload)
}

// forget drops a subscription and reports whether it was the last one holding
// that identifier, which is when the server needs to hear about it.
func (c *Client) forget(subscription *Subscription) bool {
	c.mu.Lock()
	remaining := []*Subscription{}
	for _, candidate := range c.subscriptions[subscription.identifier] {
		if candidate != subscription {
			remaining = append(remaining, candidate)
		}
	}
	last := len(remaining) == 0
	if last {
		delete(c.subscriptions, subscription.identifier)
	} else {
		c.subscriptions[subscription.identifier] = remaining
	}
	c.mu.Unlock()

	subscription.pending.Store(false)
	subscription.close()

	return last
}

func (c *Client) closeSubscriptions() {
	c.mu.Lock()
	subscriptions := c.allSubscriptionsLocked()
	c.subscriptions = map[string][]*Subscription{}
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.close()
	}
}

func (c *Client) allSubscriptionsLocked() []*Subscription {
	subscriptions := []*Subscription{}
	for _, identified := range c.subscriptions {
		subscriptions = append(subscriptions, identified...)
	}

	return subscriptions
}

func (c *Client) pendingSubscriptions() []*Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := []*Subscription{}
	for _, subscription := range c.allSubscriptionsLocked() {
		if subscription.pending.Load() {
			pending = append(pending, subscription)
		}
	}

	return pending
}

// stop shuts the client down for good: some failures don't get better by
// dialing again.
func (c *Client) stop(err error) error {
	c.mu.Lock()
	c.stopped = true
	if c.failure == nil {
		c.failure = err
	}
	cancel := c.cancel
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return err
}

func (c *Client) finish() {
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *Client) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.stopped
}

func (c *Client) stoppedBecause() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.failureLocked()
}

func (c *Client) failureLocked() error {
	if c.failure != nil {
		return c.failure
	} else {
		return ErrClosed
	}
}

func (c *Client) countAttempt() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.attempts++
}

// reconnectDelay doubles the delay per failed attempt, up to the longest, and
// spreads the result over the last interval so a restarted server doesn't get
// every client back at the same instant.
func (c *Client) reconnectDelay() time.Duration {
	c.mu.Lock()
	attempts := c.attempts
	c.mu.Unlock()

	delay := min(c.initialBackoff<<min(max(attempts-1, 0), 16), c.longestBackoff)

	return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
}

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
	maxAttempts    int
	messageBuffer  int

	// mu guards every field below it.
	mu   sync.Mutex
	conn Conn
	// protocol is the one the server picked for the connection in hand.
	protocol      Protocol
	subscriptions map[string]*registration
	attempts      int
	// lastErr is why the latest attempt failed, kept so a Connect that gives up
	// waiting can say what it was waiting on.
	lastErr      error
	reconnected  bool
	welcomed     bool
	everWelcomed bool
	stopped      bool
	failure      error
	ctx          context.Context
	cancel       context.CancelFunc

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
		subscriptions:  map[string]*registration{},
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
// ctx bounds the wait, not a connection that got through: that lives until Close.
// A Connect that returns an error leaves the client stopped, with nothing running
// behind it, so a client that failed to connect is one to throw away. The one
// exception is ErrAlreadyConnected, which says the client was running fine before
// the call and still is.
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
	c.ctx, c.cancel = runContext, cancel
	c.mu.Unlock()

	go c.run(runContext)

	select {
	case <-c.connected:
		return nil
	case <-c.done:
		return c.stoppedBecause()
	case <-ctx.Done():
		return c.giveUpWaiting(ctx)
	}
}

// giveUpWaiting stops a client whose Connect ran out of time, unless the welcome
// landed in the same instant, in which case the connection is kept. Both are
// settled under one lock, so a welcome can't slip in between the check and the
// stop and be torn down for its trouble.
func (c *Client) giveUpWaiting(ctx context.Context) error {
	c.mu.Lock()
	if c.everWelcomed {
		c.mu.Unlock()
		return nil
	}
	c.stopLocked(c.explainLocked(ctx.Err()))
	c.mu.Unlock()

	return c.awaitStopped()
}

// Connected reports whether a connection is up and welcomed.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.welcomed && c.conn != nil
}

// Done closes when the client has stopped for good — closed, told by the server
// not to come back, out of attempts, or unable to connect in the first place — and
// will neither reconnect nor deliver anything more. Err says why.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err reports why the client stopped, and nil while it is still running or has
// yet to be started. It is one of ErrClosed, ErrGaveUp, ErrUnsupportedSubprotocol,
// ErrNoProtocols, a *DisconnectError, or the context error a failed Connect
// returned.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return c.failureLocked()
	} else {
		return nil
	}
}

// Subscribe subscribes to a channel and returns once the server confirms it.
// The subscription outlives reconnects — it is resubscribed automatically — so it
// stays valid until Unsubscribe.
//
// Subscribing to an identifier the client already holds shares the server's one
// subscription for it instead of asking for another, which Rails would ignore.
// Every subscription sharing an identifier gets every message, and the server
// hears unsubscribe from the last one to go.
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
	registration, shared := c.subscriptions[key]
	if !shared {
		registration = newRegistration()
		c.subscriptions[key] = registration
	}
	registration.holders = append(registration.holders, subscription)
	confirmed := registration.confirmed
	c.mu.Unlock()

	if confirmed {
		// The server said yes to this identifier on the connection in hand and
		// won't say so again, so the new holder is as confirmed as the rest.
		subscription.confirm(false)
		return subscription, nil
	}

	// A shared identifier's subscribe is already out, or goes out with the next
	// welcome, and its verdict is this subscription's too.
	if !shared {
		if err := c.send(ctx, Command{Name: CommandSubscribe, Identifier: key}); err != nil {
			// Nothing to do about it here: the connection will subscribe again as
			// soon as it is welcomed back.
			c.logger.Printf("actioncable: subscribing to %s: %v", key, err)
		}
	}

	select {
	case <-subscription.confirmed:
		return subscription, nil
	case <-subscription.rejected:
		rejection := subscription.rejection()
		c.forget(subscription, rejection)
		return nil, rejection
	case <-c.done:
		failure := c.stoppedBecause()
		c.forget(subscription, failure)
		return nil, failure
	case <-ctx.Done():
		c.abandon(subscription, ctx.Err())
		return nil, ctx.Err()
	}
}

// abandon forgets a subscription its caller gave up waiting on. When it was the
// last holder of an identifier the server has heard a subscribe for, the server
// is told to let go, or it would keep the subscription and ignore the next
// Subscribe for it as a duplicate. The connection may well be gone by now, and
// then there is nothing to tell.
//
// The caller's context is what just ended, so the unsubscribe goes out on the
// client's own. It is sent before returning rather than in the background so a
// Subscribe for the same identifier that follows can't get ahead of it.
func (c *Client) abandon(subscription *Subscription, reason error) {
	if last, heard := c.forget(subscription, reason); last && heard {
		c.send(c.runContext(), Command{Name: CommandUnsubscribe, Identifier: subscription.identifier})
	}
}

// runContext is the one the connection runs under, which outlives any the caller
// holds and ends with the client.
func (c *Client) runContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ctx
}

// Close hangs up, stops reconnecting, and closes every subscription's message
// channel. It is safe to call from a subscription callback, and safe to call
// twice.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)

	return nil
}

// shutdown stops the client for the reason given, hangs up whatever connection
// is open, and waits until nothing is running any more. It returns why the client
// stopped, which is an earlier reason when there was one.
func (c *Client) shutdown(reason error) error {
	c.mu.Lock()
	c.stopLocked(reason)
	c.mu.Unlock()

	return c.awaitStopped()
}

// awaitStopped hangs up whatever connection a stopped client still has open and
// waits until nothing is running any more.
func (c *Client) awaitStopped() error {
	c.mu.Lock()
	failure, cancel, conn := c.failure, c.cancel, c.conn
	c.mu.Unlock()

	if cancel == nil {
		// Nothing was ever started, so nothing will finish it for us.
		c.finish()
		return failure
	}

	cancel()
	if conn != nil {
		conn.Close()
	}
	<-c.done

	return failure
}

func (c *Client) run(ctx context.Context) {
	defer c.finish()
	defer c.closeSubscriptions()

	for {
		err := c.session(ctx)
		if err != nil && !c.isStopped() {
			c.logger.Printf("actioncable: connection to %s ended: %v", c.url, err)
		}

		if c.isStopped() || ctx.Err() != nil {
			return
		}

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
		return c.failed(ctx, err)
	}

	conn, err := c.transport.Dial(ctx, c.url, DialOptions{
		Subprotocols: append(c.subprotocols(), SubprotocolUnsupported),
		Header:       header,
	})
	if err != nil {
		return c.failed(ctx, err)
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

	return c.failed(ctx, c.receive(ctx, conn, protocol))
}

// failed records why an attempt ended and, when that was the last one allowed,
// stops the client. It runs ahead of the deferred disconnect so the subscriptions
// hear that the client is not coming back rather than that it is.
func (c *Client) failed(ctx context.Context, err error) error {
	if c.isStopped() || ctx.Err() != nil {
		return err
	}

	if c.countAttempt(err) == c.maxAttempts {
		c.stop(c.explain(ErrGaveUp))
	}

	return err
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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	c.attempts = 0
	c.welcomed = true
	c.reconnected = c.everWelcomed
	c.everWelcomed = true
	identifiers := make([]string, 0, len(c.subscriptions))
	for identifier, registration := range c.subscriptions {
		registration.pending, registration.confirmed = true, false
		identifiers = append(identifiers, identifier)
	}
	c.mu.Unlock()

	c.connectedOnce.Do(func() { close(c.connected) })

	c.resubscribeLocked(ctx, identifiers)
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
			c.writeMu.Lock()
			c.resubscribeLocked(ctx, c.pendingIdentifiers())
			c.writeMu.Unlock()
		}
	}
}

// resubscribeLocked sends a subscribe for each identifier. The caller holds
// writeMu from before the identifiers were listed until this returns, so nothing
// else can get a command out in between. Otherwise an Unsubscribe that lands
// mid-list could write its unsubscribe ahead of the subscribe for the same
// identifier, and the server would end up holding a subscription nobody here
// knows about — one it would silently ignore every later subscribe for.
func (c *Client) resubscribeLocked(ctx context.Context, identifiers []string) {
	for _, identifier := range identifiers {
		if err := c.write(ctx, Command{Name: CommandSubscribe, Identifier: identifier}); err != nil {
			c.logger.Printf("actioncable: resubscribing to %s: %v", identifier, err)
		}
	}
}

func (c *Client) confirm(identifier string) {
	c.mu.Lock()
	registration, reconnected := c.subscriptions[identifier], c.reconnected
	// Only an identifier waiting on a verdict has news. The server can confirm
	// twice when a retried subscribe crosses the first confirmation.
	if registration == nil || !registration.pending {
		c.mu.Unlock()
		return
	}
	registration.pending, registration.confirmed = false, true
	holders := registration.holders
	c.mu.Unlock()

	for _, subscription := range holders {
		subscription.confirm(reconnected)
	}
}

func (c *Client) reject(identifier string) {
	c.mu.Lock()
	holders := c.holdersLocked(identifier)
	delete(c.subscriptions, identifier)
	c.mu.Unlock()

	for _, subscription := range holders {
		subscription.reject()
	}
}

func (c *Client) deliver(incoming Incoming) {
	c.mu.Lock()
	subscriptions := c.holdersLocked(incoming.Identifier)
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
	for _, registration := range c.subscriptions {
		registration.pending, registration.confirmed = false, false
	}
	subscriptions := c.allSubscriptionsLocked()
	willReconnect := !c.stopped
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.disconnect(willReconnect)
	}
}

func (c *Client) send(ctx context.Context, command Command) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.write(ctx, command)
}

// write puts one command on the connection. The caller holds writeMu.
func (c *Client) write(ctx context.Context, command Command) error {
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

	return conn.Write(ctx, payload)
}

// forget drops a subscription and reports whether it was the last one holding
// that identifier, which is when the server needs to hear about it, and whether
// the server has heard a subscribe for it on the connection in hand at all.
func (c *Client) forget(subscription *Subscription, reason error) (last, heard bool) {
	c.mu.Lock()
	remaining := []*Subscription{}
	for _, candidate := range c.holdersLocked(subscription.identifier) {
		if candidate != subscription {
			remaining = append(remaining, candidate)
		}
	}
	last = len(remaining) == 0
	if registration := c.subscriptions[subscription.identifier]; registration != nil {
		heard = registration.pending || registration.confirmed
	}
	if last {
		delete(c.subscriptions, subscription.identifier)
	} else {
		c.subscriptions[subscription.identifier].holders = remaining
	}
	c.mu.Unlock()

	subscription.close(reason)

	return last, heard
}

func (c *Client) closeSubscriptions() {
	c.mu.Lock()
	subscriptions := c.allSubscriptionsLocked()
	c.subscriptions = map[string]*registration{}
	failure := c.failureLocked()
	c.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.close(failure)
	}
}

func (c *Client) holdersLocked(identifier string) []*Subscription {
	if registration := c.subscriptions[identifier]; registration != nil {
		return registration.holders
	} else {
		return nil
	}
}

func (c *Client) allSubscriptionsLocked() []*Subscription {
	subscriptions := []*Subscription{}
	for _, registration := range c.subscriptions {
		subscriptions = append(subscriptions, registration.holders...)
	}

	return subscriptions
}

func (c *Client) pendingIdentifiers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := []string{}
	for identifier, registration := range c.subscriptions {
		if registration.pending {
			pending = append(pending, identifier)
		}
	}

	return pending
}

// stop shuts the client down for good: some failures don't get better by
// dialing again.
func (c *Client) stop(err error) error {
	c.mu.Lock()
	c.stopLocked(err)
	cancel := c.cancel
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return err
}

// stopLocked marks the client stopped for the reason given, unless an earlier
// reason already stands.
func (c *Client) stopLocked(reason error) {
	c.stopped = true
	if c.failure == nil {
		c.failure = reason
	}
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

// countAttempt records one more failed attempt, and what failed it, and reports
// how many have failed in a row.
func (c *Client) countAttempt(err error) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.attempts++
	c.lastErr = err

	return c.attempts
}

// explain pairs an error about giving up with the failure that was being waited
// out, so a deadline that ran out on bad credentials says so.
func (c *Client) explain(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.explainLocked(err)
}

func (c *Client) explainLocked(err error) error {
	if c.lastErr != nil {
		return fmt.Errorf("%w (last attempt: %w)", err, c.lastErr)
	} else {
		return err
	}
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

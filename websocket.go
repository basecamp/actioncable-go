package actioncable

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// WebSocketTransport is the built-in transport: an RFC 6455 client written on
// the standard library, so the package carries no dependencies. It handles the
// upgrade handshake, masks what it sends, answers pings, and reassembles
// fragmented messages.
type WebSocketTransport struct {
	// Dialer opens the TCP connection. A zero value dialer is used when nil.
	Dialer *net.Dialer

	// TLSConfig configures wss:// connections.
	TLSConfig *tls.Config

	// HandshakeTimeout bounds the upgrade request. Defaults to 10 seconds.
	HandshakeTimeout time.Duration

	// WriteTimeout bounds a single write when the caller's context has no
	// deadline. Defaults to 10 seconds.
	WriteTimeout time.Duration

	// MaxMessageSize is the largest message accepted, in bytes. Defaults to 8 MB.
	MaxMessageSize int64
}

const webSocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa
)

func (t *WebSocketTransport) Dial(ctx context.Context, rawURL string, options DialOptions) (Conn, error) {
	endpoint, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("actioncable: parsing %q: %w", rawURL, err)
	}

	address, secure, err := endpointAddress(endpoint)
	if err != nil {
		return nil, err
	}

	if _, deadlined := ctx.Deadline(); !deadlined {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, or(t.HandshakeTimeout, 10*time.Second))
		defer cancel()
	}

	socket, err := t.dialer().DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("actioncable: dialing %s: %w", address, err)
	}

	if secure {
		socket, err = t.handshakeTLS(ctx, socket, endpoint.Hostname())
		if err != nil {
			return nil, err
		}
	}

	conn, err := t.upgrade(ctx, socket, endpoint, options)
	if err != nil {
		socket.Close()
		return nil, err
	}

	return conn, nil
}

func endpointAddress(endpoint *url.URL) (address string, secure bool, err error) {
	switch strings.ToLower(endpoint.Scheme) {
	case "ws", "http":
		secure = false
	case "wss", "https":
		secure = true
	default:
		return "", false, fmt.Errorf("actioncable: unsupported scheme %q", endpoint.Scheme)
	}

	if port := endpoint.Port(); port != "" {
		address = endpoint.Host
	} else if secure {
		address = net.JoinHostPort(endpoint.Host, "443")
	} else {
		address = net.JoinHostPort(endpoint.Host, "80")
	}

	return address, secure, nil
}

func (t *WebSocketTransport) dialer() *net.Dialer {
	if t.Dialer != nil {
		return t.Dialer
	} else {
		return &net.Dialer{}
	}
}

func (t *WebSocketTransport) handshakeTLS(ctx context.Context, socket net.Conn, hostname string) (net.Conn, error) {
	config := t.TLSConfig
	if config == nil {
		config = &tls.Config{}
	}
	if config.ServerName == "" {
		config = config.Clone()
		config.ServerName = hostname
	}

	secured := tls.Client(socket, config)
	if err := secured.HandshakeContext(ctx); err != nil {
		socket.Close()
		return nil, fmt.Errorf("actioncable: TLS handshake with %s: %w", hostname, err)
	}

	return secured, nil
}

func (t *WebSocketTransport) upgrade(ctx context.Context, socket net.Conn, endpoint *url.URL, options DialOptions) (*webSocketConn, error) {
	if deadline, ok := ctx.Deadline(); ok {
		socket.SetDeadline(deadline)
		defer socket.SetDeadline(time.Time{})
	}

	key, err := nonce()
	if err != nil {
		return nil, err
	}

	if err := writeUpgradeRequest(socket, endpoint, key, options); err != nil {
		return nil, fmt.Errorf("actioncable: writing upgrade request: %w", err)
	}

	reader := bufio.NewReader(socket)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("actioncable: reading upgrade response: %w", err)
	}
	defer response.Body.Close()

	if err := verifyUpgrade(response, key); err != nil {
		return nil, err
	}

	return &webSocketConn{
		socket:         socket,
		reader:         reader,
		subprotocol:    response.Header.Get("Sec-WebSocket-Protocol"),
		writeTimeout:   or(t.WriteTimeout, 10*time.Second),
		maxMessageSize: or(t.MaxMessageSize, 8<<20),
	}, nil
}

func writeUpgradeRequest(socket net.Conn, endpoint *url.URL, key string, options DialOptions) error {
	request := &strings.Builder{}

	target := endpoint.RequestURI()
	if target == "" {
		target = "/"
	}
	fmt.Fprintf(request, "GET %s HTTP/1.1\r\n", target)
	fmt.Fprintf(request, "Host: %s\r\n", endpoint.Host)
	fmt.Fprintf(request, "Upgrade: websocket\r\n")
	fmt.Fprintf(request, "Connection: Upgrade\r\n")
	fmt.Fprintf(request, "Sec-WebSocket-Key: %s\r\n", key)
	fmt.Fprintf(request, "Sec-WebSocket-Version: 13\r\n")
	if len(options.Subprotocols) > 0 {
		fmt.Fprintf(request, "Sec-WebSocket-Protocol: %s\r\n", strings.Join(options.Subprotocols, ", "))
	}
	for name, values := range options.Header {
		if reservedHeader(name) {
			continue
		}
		for _, value := range values {
			fmt.Fprintf(request, "%s: %s\r\n", name, value)
		}
	}
	fmt.Fprintf(request, "\r\n")

	_, err := io.WriteString(socket, request.String())
	return err
}

func reservedHeader(name string) bool {
	switch http.CanonicalHeaderKey(name) {
	case "Host", "Upgrade", "Connection", "Sec-Websocket-Key", "Sec-Websocket-Version", "Sec-Websocket-Protocol", "Sec-Websocket-Extensions":
		return true
	default:
		return false
	}
}

func verifyUpgrade(response *http.Response, key string) error {
	if response.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("actioncable: server refused the upgrade with %s", response.Status)
	}
	if !strings.EqualFold(response.Header.Get("Upgrade"), "websocket") {
		return fmt.Errorf("actioncable: server did not upgrade to websocket (Upgrade: %q)", response.Header.Get("Upgrade"))
	}
	if !headerContains(response.Header.Get("Connection"), "upgrade") {
		return fmt.Errorf("actioncable: server did not upgrade the connection (Connection: %q)", response.Header.Get("Connection"))
	}
	if accepted := response.Header.Get("Sec-WebSocket-Accept"); accepted != acceptKey(key) {
		return fmt.Errorf("actioncable: server sent a bad Sec-WebSocket-Accept: %q", accepted)
	}
	if extensions := response.Header.Get("Sec-WebSocket-Extensions"); extensions != "" {
		return fmt.Errorf("actioncable: server negotiated unrequested extensions: %q", extensions)
	}

	return nil
}

func headerContains(header, token string) bool {
	for _, value := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(value), token) {
			return true
		}
	}

	return false
}

func nonce() (string, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("actioncable: generating a websocket key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(key), nil
}

func acceptKey(key string) string {
	digest := sha1.Sum([]byte(key + webSocketGUID))
	return base64.StdEncoding.EncodeToString(digest[:])
}

type webSocketConn struct {
	socket      net.Conn
	reader      *bufio.Reader
	subprotocol string

	writeTimeout   time.Duration
	maxMessageSize int64

	writeMu   sync.Mutex
	closeSent bool
	closeOnce sync.Once
}

func (c *webSocketConn) Subprotocol() string {
	return c.subprotocol
}

func (c *webSocketConn) Read(ctx context.Context) ([]byte, error) {
	release := c.watch(ctx, c.socket.SetReadDeadline, 0)
	defer release()

	var message []byte
	var fragmented bool

	for {
		frame, err := c.readFrame()
		if err != nil {
			return nil, err
		}

		switch frame.opcode {
		case opText, opBinary:
			if fragmented {
				return nil, c.failf("received a new data frame in the middle of a fragmented message")
			}
			message = frame.payload
			if frame.final {
				return message, nil
			}
			fragmented = true
		case opContinuation:
			if !fragmented {
				return nil, c.failf("received a continuation frame outside a fragmented message")
			}
			if int64(len(message)+len(frame.payload)) > c.maxMessageSize {
				return nil, c.failf("message larger than %d bytes", c.maxMessageSize)
			}
			message = append(message, frame.payload...)
			if frame.final {
				return message, nil
			}
		case opPing:
			if err := c.writeFrame(ctx, opPong, frame.payload); err != nil {
				return nil, err
			}
		case opPong:
		case opClose:
			// One close frame in reply, then the socket goes: Close sees the
			// reply was already sent and won't send a second one.
			c.writeFrame(ctx, opClose, closePayload(frame.payload))
			c.Close()
			return nil, closeErrorFrom(frame.payload)
		default:
			return nil, c.failf("received unknown opcode %#x", frame.opcode)
		}
	}
}

func (c *webSocketConn) Write(ctx context.Context, payload []byte) error {
	return c.writeFrame(ctx, opText, payload)
}

func (c *webSocketConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		if !c.closeSent {
			c.socket.SetWriteDeadline(time.Now().Add(time.Second))
			c.writeMasked(opClose, closePayload(nil))
		}
		c.writeMu.Unlock()

		err = c.socket.Close()
	})

	return err
}

type webSocketFrame struct {
	final   bool
	opcode  byte
	payload []byte
}

func (c *webSocketConn) readFrame() (webSocketFrame, error) {
	var header [2]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return webSocketFrame{}, err
	}

	frame := webSocketFrame{
		final:  header[0]&0x80 != 0,
		opcode: header[0] & 0x0f,
	}
	if header[0]&0x70 != 0 {
		return webSocketFrame{}, c.failf("received a frame with reserved bits set")
	}

	// RFC 6455 §5.1: a server must not mask what it sends, and a client that
	// receives a masked frame must fail the connection.
	if header[1]&0x80 != 0 {
		return webSocketFrame{}, c.failf("received a masked frame from the server")
	}

	length := int64(header[1] & 0x7f)
	switch length {
	case 126:
		var extended [2]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return webSocketFrame{}, err
		}
		length = int64(binary.BigEndian.Uint16(extended[:]))
	case 127:
		var extended [8]byte
		if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
			return webSocketFrame{}, err
		}
		length = int64(binary.BigEndian.Uint64(extended[:]) & 0x7fffffffffffffff)
	}

	if frame.opcode >= opClose {
		if !frame.final || length > 125 {
			return webSocketFrame{}, c.failf("received a fragmented or oversized control frame")
		}
	}
	if length > c.maxMessageSize {
		return webSocketFrame{}, c.failf("frame larger than %d bytes", c.maxMessageSize)
	}

	frame.payload = make([]byte, length)
	if _, err := io.ReadFull(c.reader, frame.payload); err != nil {
		return webSocketFrame{}, err
	}

	return frame, nil
}

func (c *webSocketConn) writeFrame(ctx context.Context, opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	release := c.watch(ctx, c.socket.SetWriteDeadline, c.writeTimeout)
	defer release()

	return c.writeMasked(opcode, payload)
}

func (c *webSocketConn) writeMasked(opcode byte, payload []byte) error {
	if opcode == opClose {
		c.closeSent = true
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return fmt.Errorf("actioncable: generating a frame mask: %w", err)
	}

	header := make([]byte, 0, 14)
	header = append(header, 0x80|opcode)
	switch length := len(payload); {
	case length <= 125:
		header = append(header, 0x80|byte(length))
	case length <= 0xffff:
		header = append(header, 0x80|126)
		header = binary.BigEndian.AppendUint16(header, uint16(length))
	default:
		header = append(header, 0x80|127)
		header = binary.BigEndian.AppendUint64(header, uint64(length))
	}
	header = append(header, mask[:]...)

	masked := make([]byte, len(payload))
	copy(masked, payload)
	applyMask(mask, masked)

	if _, err := c.socket.Write(append(header, masked...)); err != nil {
		return fmt.Errorf("actioncable: writing frame: %w", err)
	}

	return nil
}

func applyMask(mask [4]byte, payload []byte) {
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
}

func closePayload(received []byte) []byte {
	code := uint16(1000)
	if len(received) >= 2 {
		// Echo the code back only when we're allowed to send it ourselves:
		// normal, going away, or an application's own.
		if echoed := binary.BigEndian.Uint16(received); echoed >= 3000 || echoed == 1000 || echoed == 1001 {
			code = echoed
		}
	}

	return binary.BigEndian.AppendUint16(nil, code)
}

func closeErrorFrom(payload []byte) error {
	if len(payload) < 2 {
		return io.EOF
	} else {
		return fmt.Errorf("actioncable: server closed the connection: %d %s", binary.BigEndian.Uint16(payload), payload[2:])
	}
}

// watch bounds a blocking call on the socket. The deadline comes from ctx, or
// from fallback when ctx has none, and cancelling ctx trips the deadline so the
// blocked read or write returns instead of hanging.
//
// This is context.AfterFunc with one addition: releasing waits for the watcher
// to be gone. AfterFunc's stop can't do that — it reports that the func is
// already running and leaves it running — and a watcher outliving its call would
// trip the deadline of the next one.
func (c *webSocketConn) watch(ctx context.Context, setDeadline func(time.Time) error, fallback time.Duration) func() {
	if deadline, ok := ctx.Deadline(); ok {
		setDeadline(deadline)
	} else if fallback > 0 {
		setDeadline(time.Now().Add(fallback))
	} else {
		setDeadline(time.Time{})
	}

	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)

		select {
		case <-ctx.Done():
			setDeadline(time.Now())
		case <-done:
		}
	}()

	// Waiting for the watcher to be gone matters: a watcher still running after
	// the call it belonged to could trip the deadline of the next one.
	return func() {
		close(done)
		<-stopped
	}
}

// failf fails the connection: a frame we can't trust means the peer isn't
// speaking the protocol, and reading on would be guesswork.
func (c *webSocketConn) failf(reason string, args ...any) error {
	c.Close()
	return fmt.Errorf("actioncable: "+reason, args...)
}

func or[T int64 | time.Duration](value, fallback T) T {
	if value > 0 {
		return value
	} else {
		return fallback
	}
}

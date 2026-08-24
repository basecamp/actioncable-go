package actioncable

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketTransportNegotiatesTheSubprotocol(t *testing.T) {
	server := newTestServer(t)

	conn := dial(t, server, DialOptions{Subprotocols: []string{SubprotocolV1JSON}})
	defer conn.Close()

	assert.Equal(t, SubprotocolV1JSON, conn.Subprotocol())
	assert.Equal(t, SubprotocolV1JSON, server.accept(t).request.Header.Get("Sec-WebSocket-Protocol"), "expected the client to offer the subprotocol")
}

func TestWebSocketTransportSendsHeaders(t *testing.T) {
	server := newTestServer(t)

	conn := dial(t, server, DialOptions{
		Subprotocols: []string{SubprotocolV1JSON},
		Header:       http.Header{"Cookie": {"session=secret"}, "Origin": {"https://example.com"}},
	})
	defer conn.Close()

	request := server.accept(t).request
	assert.Equal(t, "session=secret", request.Header.Get("Cookie"))
	assert.Equal(t, "https://example.com", request.Header.Get("Origin"))
	assert.Equal(t, "/cable", request.URL.Path)
}

func TestWebSocketTransportRoundTripsMessages(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{Subprotocols: []string{SubprotocolV1JSON}})
	defer conn.Close()
	peer := server.accept(t)

	require.NoError(t, conn.Write(context.Background(), []byte(`{"command":"subscribe"}`)), "Write")
	assert.Equal(t, `{"command":"subscribe"}`, peer.read(t))

	peer.write(t, opText, []byte(`{"type":"welcome"}`))
	assert.Equal(t, `{"type":"welcome"}`, string(read(t, conn)))
}

func TestWebSocketTransportAnswersPings(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	peer.write(t, opPing, []byte("beat"))
	peer.write(t, opText, []byte("after the ping"))

	assert.Equal(t, "after the ping", string(read(t, conn)))

	frame := peer.readFrame(t)
	assert.Equal(t, byte(opPong), frame.opcode, "expected a pong")
	assert.Equal(t, "beat", string(frame.payload), "expected the pong to carry the ping payload")
}

func TestWebSocketTransportReassemblesFragments(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	peer.writeFragment(t, opText, []byte("one "), false)
	peer.writeFragment(t, opPing, []byte("interleaved"), true)
	peer.writeFragment(t, opContinuation, []byte("message"), true)

	assert.Equal(t, "one message", string(read(t, conn)))
}

func TestWebSocketTransportReadsLargeMessages(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	long := strings.Repeat("cable", 30_000)
	peer.write(t, opText, []byte(long))
	assert.Equal(t, long, string(read(t, conn)))

	require.NoError(t, conn.Write(context.Background(), []byte(long)), "Write")
	assert.Equal(t, long, peer.read(t))
}

func TestWebSocketTransportRefusesOversizedMessages(t *testing.T) {
	server := newTestServer(t)
	transport := &WebSocketTransport{MaxMessageSize: 8}

	conn, err := transport.Dial(context.Background(), server.url(), DialOptions{})
	require.NoError(t, err, "Dial")
	defer conn.Close()

	server.accept(t).write(t, opText, []byte("far too long for eight bytes"))
	_, err = readWithin(conn)
	require.Error(t, err, "expected an error for an oversized message")
}

func TestWebSocketTransportReportsServerClose(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()

	server.accept(t).write(t, opClose, binary.BigEndian.AppendUint16(nil, 1001))

	_, err := readWithin(conn)
	require.Error(t, err, "expected an error after the server closed")
}

func TestWebSocketTransportRefusesANonUpgradeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no cable here", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	transport := &WebSocketTransport{}
	_, err := transport.Dial(context.Background(), websocketURL(server.URL), DialOptions{})
	require.Error(t, err, "expected an error for a server that refuses to upgrade")
}

func TestWebSocketTransportRefusesABadAcceptKey(t *testing.T) {
	server := newTestServer(t)
	server.badAccept = true

	transport := &WebSocketTransport{}
	_, err := transport.Dial(context.Background(), server.url(), DialOptions{})
	require.Error(t, err, "expected an error for a bad Sec-WebSocket-Accept")
}

func TestWebSocketTransportHonorsContextCancellation(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	server.accept(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := conn.Read(ctx)
	require.Error(t, err, "expected Read to give up with the context")
}

// TestClientOverTheRealTransport runs the whole cable dance over an actual
// WebSocket connection.
func TestClientOverTheRealTransport(t *testing.T) {
	server := newTestServer(t)
	client := New(server.url(), WithLogger(testLogger(t)))
	t.Cleanup(func() { client.Close() })

	connecting := connect(client)
	peer := server.accept(t)
	peer.write(t, opText, []byte(`{"type":"welcome"}`))
	require.NoError(t, <-connecting, "Connect")

	subscribing := subscribe(client, room())
	assert.Equal(t, `{"command":"subscribe","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}"}`, peer.read(t))
	peer.write(t, opText, []byte(`{"type":"confirm_subscription","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}"}`))

	result := <-subscribing
	require.NoError(t, result.err, "Subscribe")
	subscription := result.subscription

	peer.write(t, opText, []byte(`{"identifier":"{\"channel\":\"RoomChannel\",\"id\":42}","message":{"body":"Hello!"}}`))
	assert.Equal(t, `{"body":"Hello!"}`, receive(t, subscription).String())

	require.NoError(t, subscription.Perform(context.Background(), "speak", map[string]any{"body": "Hi!"}), "Perform")
	assert.Equal(t, `{"command":"message","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}","data":"{\"action\":\"speak\",\"body\":\"Hi!\"}"}`, peer.read(t))
}

func dial(t *testing.T, server *testServer, options DialOptions) Conn {
	t.Helper()

	conn, err := (&WebSocketTransport{}).Dial(context.Background(), server.url(), options)
	require.NoError(t, err, "Dial")

	return conn
}

func read(t *testing.T, conn Conn) []byte {
	t.Helper()

	payload, err := readWithin(conn)
	require.NoError(t, err, "Read")

	return payload
}

func readWithin(conn Conn) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	return conn.Read(ctx)
}

// testServer speaks just enough of the server side of RFC 6455 to exercise the
// transport: it completes the handshake and then hands the raw connection over.
type testServer struct {
	*httptest.Server
	accepted  chan *peerConn
	badAccept bool
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()

	server := &testServer{accepted: make(chan *peerConn, 4)}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, reader, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijacking: %v", err)
			return
		}

		accepted := acceptKey(r.Header.Get("Sec-WebSocket-Key"))
		if server.badAccept {
			accepted = "obviously-wrong"
		}

		response := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accepted + "\r\n"
		if offered := r.Header.Get("Sec-WebSocket-Protocol"); offered != "" {
			response += "Sec-WebSocket-Protocol: " + strings.TrimSpace(strings.Split(offered, ",")[0]) + "\r\n"
		}
		response += "\r\n"

		if _, err := io.WriteString(socket, response); err != nil {
			t.Errorf("writing the handshake: %v", err)
			return
		}

		server.accepted <- &peerConn{socket: socket, reader: reader.Reader, request: r}
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)

	return server
}

func (s *testServer) url() string {
	return websocketURL(s.Server.URL) + "/cable"
}

func (s *testServer) accept(t *testing.T) *peerConn {
	t.Helper()

	select {
	case conn := <-s.accepted:
		return conn
	case <-time.After(wait):
		t.Fatal("no client connected")
		return nil
	}
}

func websocketURL(url string) string {
	return "ws" + strings.TrimPrefix(url, "http")
}

type peerConn struct {
	socket  net.Conn
	reader  *bufio.Reader
	request *http.Request
}

func (c *peerConn) read(t *testing.T) string {
	t.Helper()

	frame := c.readFrame(t)
	require.Equal(t, byte(opText), frame.opcode, "expected a text frame")

	return string(frame.payload)
}

func (c *peerConn) readFrame(t *testing.T) webSocketFrame {
	t.Helper()

	frame, err := c.tryReadFrame()
	require.NoError(t, err, "reading a frame")

	return frame
}

func (c *peerConn) tryReadFrame() (webSocketFrame, error) {
	c.socket.SetReadDeadline(time.Now().Add(wait))

	var header [2]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return webSocketFrame{}, err
	}

	frame := webSocketFrame{final: header[0]&0x80 != 0, opcode: header[0] & 0x0f}
	if header[1]&0x80 == 0 {
		return webSocketFrame{}, errors.New("client sent an unmasked frame")
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
		length = int64(binary.BigEndian.Uint64(extended[:]))
	}

	var mask [4]byte
	if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
		return webSocketFrame{}, err
	}

	frame.payload = make([]byte, length)
	if _, err := io.ReadFull(c.reader, frame.payload); err != nil {
		return webSocketFrame{}, err
	}
	applyMask(mask, frame.payload)

	return frame, nil
}

func (c *peerConn) write(t *testing.T, opcode byte, payload []byte) {
	t.Helper()
	c.writeFragment(t, opcode, payload, true)
}

func (c *peerConn) writeFragment(t *testing.T, opcode byte, payload []byte, final bool) {
	t.Helper()

	header := []byte{opcode}
	if final {
		header[0] |= 0x80
	}
	switch length := len(payload); {
	case length <= 125:
		header = append(header, byte(length))
	case length <= 0xffff:
		header = append(header, 126)
		header = binary.BigEndian.AppendUint16(header, uint16(length))
	default:
		header = append(header, 127)
		header = binary.BigEndian.AppendUint64(header, uint64(length))
	}

	c.socket.SetWriteDeadline(time.Now().Add(wait))
	if _, err := c.socket.Write(append(header, payload...)); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("writing a frame: %v", err)
	}
}

// writeMasked sends a frame the way only a client is allowed to: masked.
func (c *peerConn) writeMasked(t *testing.T, opcode byte, payload []byte) {
	t.Helper()

	masked := make([]byte, len(payload))
	copy(masked, payload)
	mask := [4]byte{1, 2, 3, 4}
	applyMask(mask, masked)

	header := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	header = append(header, mask[:]...)

	c.socket.SetWriteDeadline(time.Now().Add(wait))
	if _, err := c.socket.Write(append(header, masked...)); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("writing a masked frame: %v", err)
	}
}

// closeFrames counts the close frames the client sends before it goes away.
func (c *peerConn) closeFrames(t *testing.T) int {
	t.Helper()

	closes := 0
	for {
		frame, err := c.tryReadFrame()
		if err != nil {
			return closes
		}
		if frame.opcode == opClose {
			closes++
		}
	}
}

func TestWebSocketTransportRefusesAMaskedServerFrame(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()

	// RFC 6455 §5.1: a server must never mask, and a client that sees a masked
	// frame must fail the connection rather than quietly unmask it.
	server.accept(t).writeMasked(t, opText, []byte(`{"type":"welcome"}`))

	payload, err := readWithin(conn)
	require.Error(t, err, "expected a masked frame to fail the connection, got %s", payload)
}

func TestWebSocketTransportRepliesToACloseOnce(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	peer := server.accept(t)

	peer.write(t, opClose, binary.BigEndian.AppendUint16(nil, 1000))
	_, err := readWithin(conn)
	require.Error(t, err, "expected an error after the server closed")
	conn.Close()

	assert.Equal(t, 1, peer.closeFrames(t), "expected exactly one close frame in reply")
}

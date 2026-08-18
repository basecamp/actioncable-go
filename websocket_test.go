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
)

func TestWebSocketTransportNegotiatesTheSubprotocol(t *testing.T) {
	server := newTestServer(t)

	conn := dial(t, server, DialOptions{Subprotocols: []string{SubprotocolV1JSON}})
	defer conn.Close()

	if conn.Subprotocol() != SubprotocolV1JSON {
		t.Fatalf("expected %s, got %q", SubprotocolV1JSON, conn.Subprotocol())
	}
	if requested := server.accept(t).request.Header.Get("Sec-WebSocket-Protocol"); requested != SubprotocolV1JSON {
		t.Fatalf("expected the client to offer %s, got %q", SubprotocolV1JSON, requested)
	}
}

func TestWebSocketTransportSendsHeaders(t *testing.T) {
	server := newTestServer(t)

	conn := dial(t, server, DialOptions{
		Subprotocols: []string{SubprotocolV1JSON},
		Header:       http.Header{"Cookie": {"session=secret"}, "Origin": {"https://example.com"}},
	})
	defer conn.Close()

	request := server.accept(t).request
	if cookie := request.Header.Get("Cookie"); cookie != "session=secret" {
		t.Fatalf("expected the cookie to be sent, got %q", cookie)
	}
	if origin := request.Header.Get("Origin"); origin != "https://example.com" {
		t.Fatalf("expected the origin to be sent, got %q", origin)
	}
	if request.URL.Path != "/cable" {
		t.Fatalf("expected /cable, got %q", request.URL.Path)
	}
}

func TestWebSocketTransportRoundTripsMessages(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{Subprotocols: []string{SubprotocolV1JSON}})
	defer conn.Close()
	peer := server.accept(t)

	if err := conn.Write(context.Background(), []byte(`{"command":"subscribe"}`)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if sent := peer.read(t); sent != `{"command":"subscribe"}` {
		t.Fatalf("server received %s", sent)
	}

	peer.write(t, opText, []byte(`{"type":"welcome"}`))
	if received := string(read(t, conn)); received != `{"type":"welcome"}` {
		t.Fatalf("client received %s", received)
	}
}

func TestWebSocketTransportAnswersPings(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	peer.write(t, opPing, []byte("beat"))
	peer.write(t, opText, []byte("after the ping"))

	if received := string(read(t, conn)); received != "after the ping" {
		t.Fatalf("client received %s", received)
	}
	if frame := peer.readFrame(t); frame.opcode != opPong || string(frame.payload) != "beat" {
		t.Fatalf("expected a pong carrying the ping payload, got opcode %#x %q", frame.opcode, frame.payload)
	}
}

func TestWebSocketTransportReassemblesFragments(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	peer.writeFragment(t, opText, []byte("one "), false)
	peer.writeFragment(t, opPing, []byte("interleaved"), true)
	peer.writeFragment(t, opContinuation, []byte("message"), true)

	if received := string(read(t, conn)); received != "one message" {
		t.Fatalf("client received %q", received)
	}
}

func TestWebSocketTransportReadsLargeMessages(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	peer := server.accept(t)

	long := strings.Repeat("cable", 30_000)
	peer.write(t, opText, []byte(long))
	if received := string(read(t, conn)); received != long {
		t.Fatalf("expected %d bytes, got %d", len(long), len(received))
	}

	if err := conn.Write(context.Background(), []byte(long)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if sent := peer.read(t); sent != long {
		t.Fatalf("server received %d bytes, expected %d", len(sent), len(long))
	}
}

func TestWebSocketTransportRefusesOversizedMessages(t *testing.T) {
	server := newTestServer(t)
	transport := &WebSocketTransport{MaxMessageSize: 8}

	conn, err := transport.Dial(context.Background(), server.url(), DialOptions{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	server.accept(t).write(t, opText, []byte("far too long for eight bytes"))
	if _, err := readWithin(conn); err == nil {
		t.Fatal("expected an error for an oversized message")
	}
}

func TestWebSocketTransportReportsServerClose(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()

	server.accept(t).write(t, opClose, binary.BigEndian.AppendUint16(nil, 1001))

	if _, err := readWithin(conn); err == nil {
		t.Fatal("expected an error after the server closed")
	}
}

func TestWebSocketTransportRefusesANonUpgradeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no cable here", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	transport := &WebSocketTransport{}
	if _, err := transport.Dial(context.Background(), websocketURL(server.URL), DialOptions{}); err == nil {
		t.Fatal("expected an error for a server that refuses to upgrade")
	}
}

func TestWebSocketTransportRefusesABadAcceptKey(t *testing.T) {
	server := newTestServer(t)
	server.badAccept = true

	transport := &WebSocketTransport{}
	if _, err := transport.Dial(context.Background(), server.url(), DialOptions{}); err == nil {
		t.Fatal("expected an error for a bad Sec-WebSocket-Accept")
	}
}

func TestWebSocketTransportHonorsContextCancellation(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	defer conn.Close()
	server.accept(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := conn.Read(ctx); err == nil {
		t.Fatal("expected Read to give up with the context")
	}
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
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	subscribing := subscribe(client, room())
	if command := peer.read(t); command != `{"command":"subscribe","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}"}` {
		t.Fatalf("server received %s", command)
	}
	peer.write(t, opText, []byte(`{"type":"confirm_subscription","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}"}`))

	result := <-subscribing
	if result.err != nil {
		t.Fatalf("Subscribe: %v", result.err)
	}
	subscription := result.subscription

	peer.write(t, opText, []byte(`{"identifier":"{\"channel\":\"RoomChannel\",\"id\":42}","message":{"body":"Hello!"}}`))
	if body := receive(t, subscription).String(); body != `{"body":"Hello!"}` {
		t.Fatalf("client received %s", body)
	}

	if err := subscription.Perform(context.Background(), "speak", map[string]any{"body": "Hi!"}); err != nil {
		t.Fatalf("Perform: %v", err)
	}
	if command := peer.read(t); command != `{"command":"message","identifier":"{\"channel\":\"RoomChannel\",\"id\":42}","data":"{\"action\":\"speak\",\"body\":\"Hi!\"}"}` {
		t.Fatalf("server received %s", command)
	}
}

func dial(t *testing.T, server *testServer, options DialOptions) Conn {
	t.Helper()

	conn, err := (&WebSocketTransport{}).Dial(context.Background(), server.url(), options)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	return conn
}

func read(t *testing.T, conn Conn) []byte {
	t.Helper()

	payload, err := readWithin(conn)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

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
	if frame.opcode != opText {
		t.Fatalf("expected a text frame, got opcode %#x", frame.opcode)
	}

	return string(frame.payload)
}

func (c *peerConn) readFrame(t *testing.T) webSocketFrame {
	t.Helper()

	frame, err := c.tryReadFrame()
	if err != nil {
		t.Fatalf("reading a frame: %v", err)
	}

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

	if payload, err := readWithin(conn); err == nil {
		t.Fatalf("expected a masked frame to fail the connection, got %s", payload)
	}
}

func TestWebSocketTransportRepliesToACloseOnce(t *testing.T) {
	server := newTestServer(t)
	conn := dial(t, server, DialOptions{})
	peer := server.accept(t)

	peer.write(t, opClose, binary.BigEndian.AppendUint16(nil, 1000))
	if _, err := readWithin(conn); err == nil {
		t.Fatal("expected an error after the server closed")
	}
	conn.Close()

	if closes := peer.closeFrames(t); closes != 1 {
		t.Fatalf("expected exactly one close frame in reply, got %d", closes)
	}
}

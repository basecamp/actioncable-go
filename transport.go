package actioncable

import (
	"context"
	"net/http"
)

// A Transport dials the network connection a client talks over. It is the seam
// where a network handler plugs in: the built-in WebSocketTransport speaks
// RFC 6455 on the standard library, and wrapping gorilla/websocket,
// coder/websocket, or an in-memory pipe for tests means implementing these two
// interfaces and nothing else.
type Transport interface {
	Dial(ctx context.Context, url string, options DialOptions) (Conn, error)
}

// DialOptions are what the client needs the transport to negotiate: the
// subprotocols its protocol adapter speaks, and the headers that authenticate
// the request — a cookie or a token, since an Action Cable server authorizes the
// upgrade request itself.
type DialOptions struct {
	Subprotocols []string
	Header       http.Header
}

// A Conn is one live connection. Read and Write are each called from a single
// goroutine at a time, but Close may be called concurrently with either, and
// must interrupt them.
type Conn interface {
	// Subprotocol reports what the server negotiated, empty if it named none.
	Subprotocol() string

	// Read returns the next complete message. It returns an error once the
	// connection is unusable, including when ctx is done.
	Read(ctx context.Context) ([]byte, error)

	// Write sends one text message.
	Write(ctx context.Context, payload []byte) error

	Close() error
}

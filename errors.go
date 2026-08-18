package actioncable

import "errors"

var (
	// ErrClosed is returned by a client that has been closed, or that stopped
	// because the server told it not to reconnect.
	ErrClosed = errors.New("actioncable: client closed")

	// ErrNotConnected is returned when a command can't be sent because the
	// connection is down. Subscriptions recover on their own; a Perform or Send
	// that hits this is lost and must be retried.
	ErrNotConnected = errors.New("actioncable: not connected")

	// ErrRejected is returned by Subscribe when the channel's subscribed method
	// rejected the subscription.
	ErrRejected = errors.New("actioncable: subscription rejected")

	// ErrUnsupportedSubprotocol is returned when the server negotiated a
	// subprotocol the protocol adapter doesn't speak. Reconnecting won't fix
	// that, so the client stops.
	ErrUnsupportedSubprotocol = errors.New("actioncable: unsupported subprotocol")

	// ErrAlreadyConnected is returned by Connect on a client that is already
	// running.
	ErrAlreadyConnected = errors.New("actioncable: already connected")

	// ErrNoProtocols is returned when there is nothing to offer the server,
	// which means WithProtocols was called without any protocols.
	ErrNoProtocols = errors.New("actioncable: no protocols to offer")
)

// A DisconnectError reports that the server sent a disconnect frame.
type DisconnectError struct {
	Reason    string
	Reconnect bool
}

func (e *DisconnectError) Error() string {
	return "actioncable: server disconnected: " + e.Reason
}

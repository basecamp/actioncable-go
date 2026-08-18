package actioncable

// A Protocol translates between Action Cable commands and the bytes on the wire.
// It is the seam where an Action Cable protocol plugs in.
//
// One protocol speaks one subprotocol. A client offers every protocol it was
// given and speaks the one the server picks, so supporting a new protocol means
// adding one rather than replacing the list.
//
// Implementations must be safe for concurrent use.
type Protocol interface {
	// Subprotocol is the name this protocol negotiates under.
	Subprotocol() string

	// Encode turns a command into one outgoing message.
	Encode(command Command) ([]byte, error)

	// Decode turns one incoming message into a frame the client understands.
	Decode(payload []byte) (Incoming, error)
}

// SubprotocolUnsupported is the sentinel an Action Cable server names when it
// speaks none of the subprotocols offered. The client offers it last on every
// handshake, the way Rails' own clients do, so a server with nothing in common
// can say so outright instead of leaving the subprotocol blank.
const SubprotocolUnsupported = "actioncable-unsupported"

// CommandName is the verb of a client-to-server command.
type CommandName string

const (
	CommandSubscribe   CommandName = "subscribe"
	CommandUnsubscribe CommandName = "unsubscribe"
	CommandMessage     CommandName = "message"
)

// A Command is a client-to-server message. Data carries the already encoded
// action payload and is only set for CommandMessage.
type Command struct {
	Name       CommandName
	Identifier string
	Data       string
}

// Kind is the type of a server-to-client frame.
type Kind int

const (
	KindWelcome Kind = iota
	KindPing
	KindDisconnect
	KindConfirmation
	KindRejection
	KindMessage
)

func (k Kind) String() string {
	switch k {
	case KindWelcome:
		return "welcome"
	case KindPing:
		return "ping"
	case KindDisconnect:
		return "disconnect"
	case KindConfirmation:
		return "confirm_subscription"
	case KindRejection:
		return "reject_subscription"
	case KindMessage:
		return "message"
	default:
		return "unknown"
	}
}

// An Incoming is a decoded server-to-client frame. Reason and Reconnect are
// only set on KindDisconnect, Message on KindMessage and KindPing.
type Incoming struct {
	Kind       Kind
	Identifier string
	Message    Message
	Reason     string
	Reconnect  bool
}

// Disconnect reasons an Action Cable server sends before hanging up.
const (
	ReasonUnauthorized   = "unauthorized"
	ReasonInvalidRequest = "invalid_request"
	ReasonServerRestart  = "server_restart"
	ReasonRemote         = "remote"
)

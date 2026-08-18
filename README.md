# actioncable

Go client for Rails' Action Cable.


## Installation

```bash
go get github.com/basecamp/actioncable-go
```


## Getting started

```go
// Establish a connection
client := actioncable.New("wss://example.com/cable")
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := client.Connect(ctx); err != nil {
	return err
}
defer client.Close()

// Subscribe to a channel
room, err := client.Subscribe(ctx, actioncable.Identifier{
	Channel: "RoomChannel",
	Params:  actioncable.Params{"id": 42},
})
if err != nil {
	return err
}

// Listen for incoming messages
go func() {
	for message := range room.Messages() {
		var said struct{ Body string }
		if err := message.Unmarshal(&said); err == nil {
			fmt.Println(said.Body)
		}
	}
}()

// Send messages
if err := room.Perform(ctx, "speak", map[string]any{"body": "Hello!"}); err != nil {
	return err
}
```

`Connect` opens a connection and waits for the server to acknowledge.
Failed attempts get retried automatically. Pass a context with a deadline to
control how long to wait:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := client.Connect(ctx); err != nil {
	var disconnect *actioncable.DisconnectError
	if errors.As(err, &disconnect) && disconnect.Reason == actioncable.ReasonUnauthorized {
		return signInAgain()
	} else {
		return err
	}
}
```

The errors worth handling:

- `context.DeadlineExceeded` or `context.Canceled`, when the context ends before
  the welcome arrives. The server hasn't answered yet.
- `*DisconnectError`, when the server sends a disconnect message. `Reason` is one
  of four strings:
    * `ReasonUnauthorized` — authentication or authorization failed.
    * `ReasonInvalidRequest` — the request wasn't a valid Action Cable upgrade.
    * `ReasonServerRestart` — the Rails server is restarting.
    * `ReasonRemote` — the app closed this connection with
      `ActionCable.server.disconnect`.
- `ErrUnsupportedSubprotocol`, when the server picks a protocol this client
  doesn't speak.

A disconnect message says whether the client should re-connect. Only the ones
that say no return an error. The rest are retried, so a server restart shows up
in the log and the connection returns on its own.

`Subscribe` sends the subscription and waits for the channel to confirm it. It
returns `ErrRejected` when the channel's `subscribed` method rejects it.

`Messages` closes when the subscription is unsubscribed or the client is closed,
so a range loop over it ends on its own.

Read it promptly. A subscription buffers 64 messages, and a message that arrives
while the buffer is full gets dropped and logged rather than stalling the 
connection. Set a bigger buffer if the reader can't keep up with a burst:

```go
client := actioncable.New("wss://example.com/cable",
	actioncable.WithMessageBuffer(1000),
)
```

The buffer size applies to every subscription on the client.


## Authorizing the connection

Action Cable servers can authorize connections using cookies or headers.

Use `WithCookie` to set a cookie when establishing a connection:

```go
client := actioncable.New("wss://example.com/cable",
	actioncable.WithCookie("_session_id=..."),
)
```

`WithHeader` sets any other header:

```go
client := actioncable.New("wss://example.com/cable",
	actioncable.WithHeader(http.Header{"X-Api-Token": {"..."}}),
)
```

Rails also checks the `Origin` header and rejects a request that doesn't carry
one. By default, the `Origin` is set to the server's URL, so `wss://example.com/cable`
sends `https://example.com`. 

Set it explicitly when the server sees a different scheme or host than the URL says,
behind a proxy that terminates TLS for instance:

```go
client := actioncable.New("wss://example.com/cable",
	actioncable.WithOrigin("http://example.com"),
)
```


## Callbacks

Some channels only send what's new, so a reconnect can leave a gap. Only the
client knows a reconnect happened, so `Subscribe` takes callbacks for the
connection events:

```go
room, err := client.Subscribe(ctx, identifier,
	actioncable.OnConnected(func(reconnected bool) {
		if reconnected {
			catchUp()
		}
	}),
	actioncable.OnDisconnected(func(willReconnect bool) { ... }),
	actioncable.OnRejected(func() { ... }),
)
```

`OnConnected` runs every time the server confirms the subscription. `reconnected`
is false the first time and true every time after.

`OnDisconnected` runs when the connection drops. `willReconnect` says whether the
client is coming back or has stopped for good.

`OnRejected` runs when the channel rejects the subscription.

Callbacks run on their own goroutine, one at a time, in order. `Close`,
`Subscribe`, and `Unsubscribe` all work from inside one.


## Staying connected

Rails sends a ping every three seconds and the client watches for it. After six
seconds of silence the client treats the connection as dead, drops it, and dials
again after a second, then two, then four, up to thirty. Each delay carries a
little jitter, so a restarted server doesn't get every client back at once.

Both are configurable:

```go
client := actioncable.New("wss://example.com/cable",
	actioncable.WithStaleAfter(10*time.Second),
	actioncable.WithBackoff(time.Second, 30*time.Second),
)
```

Subscriptions come back on their own. The client resubscribes all of them on the
new connection, then resends a subscribe every half second until the server
confirms it, because a subscribe that arrives before the connection is set up
gets dropped. The same `*Subscription` and the same `Messages` channel keep
working throughout.

Actions don't come back. `Perform` and `Send` return `ErrNotConnected` while the
connection is down, or up but not yet welcomed, since Rails discards anything
that arrives that early. Send it again if it matters.


## Swapping the transport

The client speaks over the standard library's WebSocket by default.

An application that already uses a WebSocket library can keep using
it by implementing two interfaces.

`Transport` has one function:

```go
type Transport interface {
	Dial(ctx context.Context, url string, options DialOptions) (Conn, error)
}
```

`Dial` opens one connection. `options` carries the sub-protocols the client's
protocols negotiate under, and the headers that authorize the request.

`Conn` has four:

```go
type Conn interface {
	Subprotocol() string
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, payload []byte) error
	Close() error
}
```

`Subprotocol` returns the sub-protocol the server picked, empty if it picked
none. `Read` returns the next complete message. `Write` sends one text message.
`Close` hangs up, and has to interrupt a `Read` or `Write` running at the time.

Implement both and pass the transport to the client:

```go
type coderTransport struct{}

func (coderTransport) Dial(ctx context.Context, url string, options actioncable.DialOptions) (actioncable.Conn, error) {
	socket, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: options.Subprotocols,
		HTTPHeader:   options.Header,
	})
	if err != nil {
		return nil, err
	}

	return &coderConn{socket}, nil
}

type coderConn struct {
	socket *websocket.Conn
}

func (c *coderConn) Subprotocol() string {
	return c.socket.Subprotocol()
}

func (c *coderConn) Read(ctx context.Context) ([]byte, error) {
	_, payload, err := c.socket.Read(ctx)
	return payload, err
}

func (c *coderConn) Write(ctx context.Context, payload []byte) error {
	return c.socket.Write(ctx, websocket.MessageText, payload)
}

func (c *coderConn) Close() error {
	return c.socket.CloseNow()
}

client := actioncable.New(url, actioncable.WithTransport(coderTransport{}))
```

The default is `WebSocketTransport`, which speaks RFC 6455 on the standard
library and carries no dependencies.


## Adding protocols

Action Cable servers can talk multiple protocols. Rails' default is V1-JSON
and that's what's supported out-of-the-box. But, if needed, new protocols
can be added.

The `Protocol` interface has just three functions:

```go
type Protocol interface {
	Subprotocol() string
	Encode(command Command) ([]byte, error)
	Decode(payload []byte) (Incoming, error)
}
```

`Subprotocol` returns the WebSocket sub-protocol for the protocol.
`Encode` serializes a command to the protocol's wire format, while
`Decode` does the opposite.

All protocols will be offered to the server in that order. If one protocol
is preferred over another then it should be defined first:

```go
client := actioncable.New(url, actioncable.WithProtocols(
	V2MessagePack{},
	actioncable.V1JSON{},
))
```

`WithAdditionalProtocols` is a shorthand for adding new protocols to the
default list. These protocols will get prepended to the list of supported
protocols which means that they'll be preferred.

```go
client := actioncable.New(url, actioncable.WithAdditionalProtocols(V2MessagePack{}))
```

The default is `V1JSON`, which speaks `actioncable-v1-json`, Rails' default protocol.


## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. Discussions come before issues
and pull requests.


## License

Released under the MIT License. See [LICENSE](LICENSE).

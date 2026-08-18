// Package actioncable is a client for Rails' Action Cable.
//
// A Client owns one WebSocket connection to an Action Cable server and
// multiplexes any number of channel subscriptions over it. It keeps the
// connection alive the way the official JavaScript client does: the server beats
// a ping every three seconds, and a connection that goes quiet for longer than
// [WithStaleAfter] is torn down and redialed with backoff. Subscriptions survive
// reconnects — they are resubscribed as soon as the server says welcome.
//
//	client := actioncable.New("wss://example.com/cable")
//	if err := client.Connect(ctx); err != nil {
//		return err
//	}
//	defer client.Close()
//
//	room, err := client.Subscribe(ctx, actioncable.Identifier{
//		Channel: "RoomChannel",
//		Params:  actioncable.Params{"id": 42},
//	})
//	if err != nil {
//		return err
//	}
//
//	go func() {
//		for message := range room.Messages() {
//			var said struct{ Body string }
//			message.Unmarshal(&said)
//			fmt.Println(said.Body)
//		}
//	}()
//
//	room.Perform(ctx, "speak", map[string]any{"body": "Hello!"})
//
// Two things are pluggable. A [Transport] carries bytes — the built-in
// [WebSocketTransport] speaks RFC 6455 over the standard library, and any
// WebSocket package can be dropped in behind the same interface. A [Protocol]
// speaks one Action Cable wire format, negotiated as one WebSocket subprotocol —
// [V1JSON] implements actioncable-v1-json, and a new format is a new Protocol
// rather than a fork of this client. [WithProtocols] offers several, and the
// server picks the one it knows.
package actioncable

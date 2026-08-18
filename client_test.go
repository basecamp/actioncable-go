package actioncable

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"
)

var roomIdentifier = `{"channel":"RoomChannel","id":42}`

func TestConnectWaitsForTheWelcome(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)

	connecting := connect(client)
	conn := transport.accept(t)

	select {
	case err := <-connecting:
		t.Fatalf("Connect returned before the welcome: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	conn.welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.Connected() {
		t.Fatal("client is not connected after the welcome")
	}
}

func TestConnectRetriesUntilTheServerAnswers(t *testing.T) {
	transport := newFakeTransport()
	transport.failNextDial(errors.New("connection refused"))
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	connecting := connect(client)
	transport.accept(t).welcome(t)

	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

func TestSubscribeReceivesMessages(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	connections := make(chan bool, 1)
	subscribing := subscribe(client, room(), OnConnected(func(reconnected bool) { connections <- reconnected }))

	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)

	result := <-subscribing
	if result.err != nil {
		t.Fatalf("Subscribe: %v", result.err)
	}
	subscription := result.subscription
	if reconnected := <-connections; reconnected {
		t.Fatal("first connection reported itself as a reconnect")
	}

	conn.push(t, `{"identifier":`+quote(roomIdentifier)+`,"message":{"body":"Hello!"}}`)

	var said struct{ Body string }
	if err := receive(t, subscription).Unmarshal(&said); err != nil {
		t.Fatalf("decoding the message: %v", err)
	}
	if said.Body != "Hello!" {
		t.Fatalf("expected body Hello!, got %q", said.Body)
	}
}

func TestSubscribeRejected(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	rejections := make(chan struct{}, 1)
	subscribing := subscribe(client, room(), OnRejected(func() { rejections <- struct{}{} }))

	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.push(t, `{"type":"reject_subscription","identifier":`+quote(roomIdentifier)+`}`)

	if err := (<-subscribing).err; !errors.Is(err, ErrRejected) {
		t.Fatalf("expected ErrRejected, got %v", err)
	}
	select {
	case <-rejections:
	case <-time.After(wait):
		t.Fatal("OnRejected was never called")
	}
}

func TestPerformSendsAnAction(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := subscription.Perform(context.Background(), "speak", map[string]any{"body": "Hello!"}); err != nil {
		t.Fatalf("Perform: %v", err)
	}

	command := conn.expectCommand(t, CommandMessage, roomIdentifier)
	if command.Data != `{"action":"speak","body":"Hello!"}` {
		t.Fatalf("expected the action alongside the data, got %s", command.Data)
	}
}

func TestSendDeliversDataWithoutAnAction(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := subscription.Send(context.Background(), map[string]any{"body": "Hello!"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	command := conn.expectCommand(t, CommandMessage, roomIdentifier)
	if command.Data != `{"body":"Hello!"}` {
		t.Fatalf("expected the data on its own, got %s", command.Data)
	}
}

func TestSendRefusesDataThatCannotEncode(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := subscription.Send(context.Background(), func() {}); err == nil {
		t.Fatal("expected an error for a payload that can't encode")
	}
}

func TestPerformRefusesDataThatIsNotAnObject(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := subscription.Perform(context.Background(), "speak", []string{"nope"}); err == nil {
		t.Fatal("expected an error for a non-object payload")
	}
}

func TestUnsubscribeClosesMessagesAndTellsTheServer(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := subscription.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)

	select {
	case _, open := <-subscription.Messages():
		if open {
			t.Fatal("messages channel is still delivering after Unsubscribe")
		}
	case <-time.After(wait):
		t.Fatal("messages channel was never closed")
	}
}

func TestReconnectResubscribes(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	connections := make(chan bool, 2)
	disconnections := make(chan bool, 1)
	subscribing := subscribe(client, room(),
		OnConnected(func(reconnected bool) { connections <- reconnected }),
		OnDisconnected(func(willReconnect bool) { disconnections <- willReconnect }),
	)
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	<-connections

	conn.Close()

	if willReconnect := <-disconnections; !willReconnect {
		t.Fatal("disconnect reported that the client would not reconnect")
	}

	reconnected := transport.accept(t)
	reconnected.welcome(t)
	reconnected.expectCommand(t, CommandSubscribe, roomIdentifier)
	reconnected.confirm(t, roomIdentifier)

	if !<-connections {
		t.Fatal("expected the confirmation after a reconnect to report reconnected")
	}
}

func TestStaleConnectionIsReplaced(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport,
		WithStaleAfter(75*time.Millisecond),
		WithBackoff(time.Millisecond, time.Millisecond),
	)

	connecting := connect(client)
	transport.accept(t).welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Say nothing at all: no pings, no messages. The connection goes stale.
	transport.accept(t).welcome(t)
}

func TestUnconfirmedSubscribeIsRetried(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithSubscribeRetry(20*time.Millisecond))
	conn := welcomed(t, client, transport)

	subscribing := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)

	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
}

func TestServerDisconnectWithoutReconnectStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	conn.push(t, `{"type":"disconnect","reason":"unauthorized","reconnect":false}`)

	transport.refuseDial(t)
	if client.Connected() {
		t.Fatal("client is still connected after being told to go away")
	}

	var disconnect *DisconnectError
	if _, err := client.Subscribe(context.Background(), room()); !errors.As(err, &disconnect) {
		t.Fatalf("expected a DisconnectError, got %v", err)
	} else if disconnect.Reason != ReasonUnauthorized {
		t.Fatalf("expected the unauthorized reason, got %q", disconnect.Reason)
	}
}

func TestServerDisconnectWithReconnectDialsAgain(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	conn.push(t, `{"type":"disconnect","reason":"server_restart","reconnect":true}`)

	transport.accept(t).welcome(t)
}

func TestClientOffersEveryProtocolAndTheSentinel(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithProtocols(V1JSON{}, fakeProtocol{subprotocol: "actioncable-v2-json", stamp: "v2:"}))

	welcomed(t, client, transport)

	offered := transport.dialedWith().Subprotocols
	expected := []string{SubprotocolV1JSON, "actioncable-v2-json", SubprotocolUnsupported}
	if !slices.Equal(offered, expected) {
		t.Fatalf("expected to offer %v, got %v", expected, offered)
	}
}

func TestAdditionalProtocolsAreOfferedFirst(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithAdditionalProtocols(fakeProtocol{subprotocol: "actioncable-v2-json", stamp: "v2:"}))

	welcomed(t, client, transport)

	offered := transport.dialedWith().Subprotocols
	expected := []string{"actioncable-v2-json", SubprotocolV1JSON, SubprotocolUnsupported}
	if !slices.Equal(offered, expected) {
		t.Fatalf("expected to offer %v, got %v", expected, offered)
	}
}

func TestClientSpeaksTheProtocolTheServerPicked(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = "actioncable-v2-json"
	client := newTestClient(t, transport, WithProtocols(V1JSON{}, fakeProtocol{subprotocol: "actioncable-v2-json", stamp: "v2:"}))

	conn := welcomed(t, client, transport)
	subscribe(client, room())

	if sent := conn.sent(t); !bytes.HasPrefix(sent, []byte("v2:")) {
		t.Fatalf("expected the negotiated protocol to encode the subscribe, got %s", sent)
	}
}

func TestUnsupportedSentinelStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = SubprotocolUnsupported
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	if err := client.Connect(context.Background()); !errors.Is(err, ErrUnsupportedSubprotocol) {
		t.Fatalf("expected ErrUnsupportedSubprotocol, got %v", err)
	}

	transport.accept(t)
	transport.refuseDial(t)
}

func TestNoProtocolsStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithProtocols())

	if err := client.Connect(context.Background()); !errors.Is(err, ErrNoProtocols) {
		t.Fatalf("expected ErrNoProtocols, got %v", err)
	}

	transport.refuseDial(t)
}

func TestUnsupportedSubprotocolStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = "actioncable-v9-telepathy"
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	if err := client.Connect(context.Background()); !errors.Is(err, ErrUnsupportedSubprotocol) {
		t.Fatalf("expected ErrUnsupportedSubprotocol, got %v", err)
	}

	transport.accept(t)
	transport.refuseDial(t)
}

func TestSubscribeBeforeConnect(t *testing.T) {
	client := newTestClient(t, newFakeTransport())

	if _, err := client.Subscribe(context.Background(), room()); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestCloseClosesSubscriptions(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, open := <-subscription.Messages(); open {
		t.Fatal("messages channel is still delivering after Close")
	}
	if err := subscription.Perform(context.Background(), "speak", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected after Close, got %v", err)
	}
}

func TestMessagesArriveOnEverySubscriptionSharingAnIdentifier(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	first := subscribed(t, client, conn)

	subscribing := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	second := <-subscribing
	if second.err != nil {
		t.Fatalf("Subscribe: %v", second.err)
	}

	conn.push(t, `{"identifier":`+quote(roomIdentifier)+`,"message":{"body":"Hello!"}}`)

	for _, subscription := range []*Subscription{first, second.subscription} {
		if body := receive(t, subscription).String(); body != `{"body":"Hello!"}` {
			t.Fatalf("expected the broadcast, got %s", body)
		}
	}

	// Only the last subscription standing tells the server to unsubscribe.
	if err := first.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	select {
	case command := <-conn.outgoing:
		t.Fatalf("expected no command while a subscription remains, got %s", command)
	case <-time.After(100 * time.Millisecond):
	}

	if err := second.subscription.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)
}

func TestConnectAfterCloseReportsWhyItStopped(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	welcomed(t, client, transport)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := client.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	transport.refuseDial(t)
}

func TestCloseBeforeConnectLeavesTheClientDead(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := client.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if client.Connected() {
		t.Fatal("a client closed before it started reports itself connected")
	}
	transport.refuseDial(t)
}

func TestCloseFromOnDisconnected(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	closing := make(chan error, 1)
	subscribing := subscribe(client, room(), OnDisconnected(func(bool) { closing <- client.Close() }))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	conn.Close()

	select {
	case err := <-closing:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(wait):
		t.Fatal("Close from OnDisconnected never returned")
	}
}

func TestSubscribeFromOnConnected(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	other := `{"channel":"OtherChannel"}`
	subscribing := subscribe(client, room(), OnConnected(func(bool) {
		go func() {
			_, err := client.Subscribe(context.Background(), Identifier{Channel: "OtherChannel"})
			if err != nil {
				t.Errorf("Subscribe from OnConnected: %v", err)
			}
		}()
	}))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	conn.expectCommand(t, CommandSubscribe, other)
	conn.confirm(t, other)
}

func TestUnsubscribeWhileMessagesArrive(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithMessageBuffer(1))
	conn := welcomed(t, client, transport)

	for range 50 {
		subscription := subscribed(t, client, conn)

		pushed := make(chan struct{})
		go func() {
			defer close(pushed)
			conn.push(t, `{"identifier":`+quote(roomIdentifier)+`,"message":{"body":"Hello!"}}`)
		}()

		if err := subscription.Unsubscribe(context.Background()); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		<-pushed
		conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)
	}
}

func TestFirstConnectionIsNotAReconnect(t *testing.T) {
	transport := newFakeTransport()
	transport.failNextDial(errors.New("connection refused"))
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	connecting := connect(client)
	conn := transport.accept(t)
	conn.welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	connections := make(chan bool, 1)
	subscribing := subscribe(client, room(), OnConnected(func(reconnected bool) { connections <- reconnected }))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if <-connections {
		t.Fatal("a first connection that took two dials reported itself as a reconnect")
	}
}

func TestPerformBeforeTheWelcomeIsRefused(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	conn.Close()
	transport.accept(t)

	// The connection is up again but not yet welcomed, and the server throws
	// away anything sent that early, so a command then is not a command landed.
	if err := subscription.Perform(context.Background(), "speak", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestRepeatedConfirmationConnectsOnce(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	connections := make(chan bool, 2)
	subscribing := subscribe(client, room(), OnConnected(func(reconnected bool) { connections <- reconnected }))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	if err := (<-subscribing).err; err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	<-connections

	conn.confirm(t, roomIdentifier)

	select {
	case <-connections:
		t.Fatal("a second confirmation reported a second connection")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOriginDefaultsToTheCableURL(t *testing.T) {
	urls := map[string]string{
		"wss://cable.example.com/cable":      "https://cable.example.com",
		"ws://cable.example.com:3000/cable":  "http://cable.example.com:3000",
		"wss://cable.example.com:8443/cable": "https://cable.example.com:8443",
	}

	// Rails compares Origin against the host it serves on, and turns down a
	// request that carries no Origin at all.
	for url, origin := range urls {
		transport := newFakeTransport()
		client := New(url, WithTransport(transport), WithLogger(testLogger(t)))
		t.Cleanup(func() { client.Close() })

		connecting := connect(client)
		transport.accept(t).welcome(t)
		if err := <-connecting; err != nil {
			t.Fatalf("Connect: %v", err)
		}

		if sent := transport.dialedWith().Header.Get("Origin"); sent != origin {
			t.Fatalf("expected %s to dial with origin %s, got %q", url, origin, sent)
		}
	}
}

func TestExplicitOriginWins(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithOrigin("https://app.example.com"))

	connecting := connect(client)
	transport.accept(t).welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if sent := transport.dialedWith().Header.Get("Origin"); sent != "https://app.example.com" {
		t.Fatalf("expected the origin given, got %q", sent)
	}
}

func TestHeaderIsCopied(t *testing.T) {
	transport := newFakeTransport()
	header := http.Header{"Cookie": {"session=secret"}}
	client := newTestClient(t, transport, WithHeader(header))

	header.Set("Cookie", "session=tampered")

	connecting := connect(client)
	transport.accept(t).welcome(t)
	if err := <-connecting; err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if sent := transport.dialedWith().Header.Get("Cookie"); sent != "session=secret" {
		t.Fatalf("expected the header as it was given, got %q", sent)
	}
}

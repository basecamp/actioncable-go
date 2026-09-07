package actioncable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, <-connecting, "Connect")
	require.True(t, client.Connected(), "client is not connected after the welcome")
}

func TestConnectRetriesUntilTheServerAnswers(t *testing.T) {
	transport := newFakeTransport()
	transport.failNextDial(errors.New("connection refused"))
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	connecting := connect(client)
	transport.accept(t).welcome(t)

	require.NoError(t, <-connecting, "Connect")
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
	require.NoError(t, result.err, "Subscribe")
	subscription := result.subscription
	assert.False(t, <-connections, "first connection reported itself as a reconnect")

	conn.push(t, `{"identifier":`+quote(roomIdentifier)+`,"message":{"body":"Hello!"}}`)

	var said struct{ Body string }
	require.NoError(t, receive(t, subscription).Unmarshal(&said), "decoding the message")
	assert.Equal(t, "Hello!", said.Body)
}

func TestSubscribeRejected(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	rejections := make(chan struct{}, 1)
	subscribing := subscribe(client, room(), OnRejected(func() { rejections <- struct{}{} }))

	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.reject(t, roomIdentifier)

	require.ErrorIs(t, (<-subscribing).err, ErrRejected)
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

	require.NoError(t, subscription.Perform(context.Background(), "speak", map[string]any{"body": "Hello!"}), "Perform")

	command := conn.expectCommand(t, CommandMessage, roomIdentifier)
	assert.Equal(t, `{"action":"speak","body":"Hello!"}`, command.Data, "expected the action alongside the data")
}

func TestSendDeliversDataWithoutAnAction(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	require.NoError(t, subscription.Send(context.Background(), map[string]any{"body": "Hello!"}), "Send")

	command := conn.expectCommand(t, CommandMessage, roomIdentifier)
	assert.Equal(t, `{"body":"Hello!"}`, command.Data, "expected the data on its own")
}

func TestSendRefusesDataThatCannotEncode(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	require.Error(t, subscription.Send(context.Background(), func() {}), "expected an error for a payload that can't encode")
}

func TestPerformRefusesDataThatIsNotAnObject(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	require.Error(t, subscription.Perform(context.Background(), "speak", []string{"nope"}), "expected an error for a non-object payload")
}

func TestUnsubscribeClosesMessagesAndTellsTheServer(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	require.NoError(t, subscription.Unsubscribe(context.Background()), "Unsubscribe")
	conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)

	select {
	case _, open := <-subscription.Messages():
		assert.False(t, open, "messages channel is still delivering after Unsubscribe")
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
	require.NoError(t, (<-subscribing).err, "Subscribe")
	<-connections

	conn.Close()

	require.True(t, <-disconnections, "disconnect reported that the client would not reconnect")

	reconnected := transport.accept(t)
	reconnected.welcome(t)
	reconnected.expectCommand(t, CommandSubscribe, roomIdentifier)
	reconnected.confirm(t, roomIdentifier)

	assert.True(t, <-connections, "expected the confirmation after a reconnect to report reconnected")
}

func TestStaleConnectionIsReplaced(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport,
		WithStaleAfter(75*time.Millisecond),
		WithBackoff(time.Millisecond, time.Millisecond),
	)

	connecting := connect(client)
	transport.accept(t).welcome(t)
	require.NoError(t, <-connecting, "Connect")

	// Say nothing at all: no pings, no messages. The connection goes stale.
	transport.accept(t).welcome(t)
}

func TestUnconfirmedSubscribeIsRetried(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithSubscribeRetry(20*time.Millisecond))
	conn := welcomed(t, client, transport)

	subscribing := subscribe(client, room())
	conn.dropCommand(t, CommandSubscribe, roomIdentifier)
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)

	conn.confirm(t, roomIdentifier)
	require.NoError(t, (<-subscribing).err, "Subscribe")
}

func TestServerDisconnectWithoutReconnectStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	conn.push(t, `{"type":"disconnect","reason":"unauthorized","reconnect":false}`)

	transport.refuseDial(t)
	assert.False(t, client.Connected(), "client is still connected after being told to go away")

	var disconnect *DisconnectError
	_, err := client.Subscribe(context.Background(), room())
	require.ErrorAs(t, err, &disconnect)
	assert.Equal(t, ReasonUnauthorized, disconnect.Reason)
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
	assert.Equal(t, []string{SubprotocolV1JSON, "actioncable-v2-json", SubprotocolUnsupported}, offered)
}

func TestAdditionalProtocolsAreOfferedFirst(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithAdditionalProtocols(fakeProtocol{subprotocol: "actioncable-v2-json", stamp: "v2:"}))

	welcomed(t, client, transport)

	offered := transport.dialedWith().Subprotocols
	assert.Equal(t, []string{"actioncable-v2-json", SubprotocolV1JSON, SubprotocolUnsupported}, offered)
}

func TestClientSpeaksTheProtocolTheServerPicked(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = "actioncable-v2-json"
	client := newTestClient(t, transport, WithProtocols(V1JSON{}, fakeProtocol{subprotocol: "actioncable-v2-json", stamp: "v2:"}))

	conn := welcomed(t, client, transport)
	subscribe(client, room())

	sent := conn.sent(t)
	assert.True(t, bytes.HasPrefix(sent, []byte("v2:")), "expected the negotiated protocol to encode the subscribe, got %s", sent)
}

func TestUnsupportedSentinelStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = SubprotocolUnsupported
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	require.ErrorIs(t, client.Connect(context.Background()), ErrUnsupportedSubprotocol)

	transport.accept(t)
	transport.refuseDial(t)
}

func TestNoProtocolsStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithProtocols())

	require.ErrorIs(t, client.Connect(context.Background()), ErrNoProtocols)

	transport.refuseDial(t)
}

func TestUnsupportedSubprotocolStopsTheClient(t *testing.T) {
	transport := newFakeTransport()
	transport.subprotocol = "actioncable-v9-telepathy"
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))

	require.ErrorIs(t, client.Connect(context.Background()), ErrUnsupportedSubprotocol)

	transport.accept(t)
	transport.refuseDial(t)
}

func TestSubscribeBeforeConnect(t *testing.T) {
	client := newTestClient(t, newFakeTransport())

	_, err := client.Subscribe(context.Background(), room())
	require.ErrorIs(t, err, ErrNotConnected)
}

func TestCloseClosesSubscriptions(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscription := subscribed(t, client, conn)

	require.NoError(t, client.Close(), "Close")

	_, open := <-subscription.Messages()
	assert.False(t, open, "messages channel is still delivering after Close")
	require.ErrorIs(t, subscription.Perform(context.Background(), "speak", nil), ErrNotConnected, "after Close")
}

func TestMessagesArriveOnEverySubscriptionSharingAnIdentifier(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	first := subscribed(t, client, conn)
	second, err := client.Subscribe(context.Background(), room())
	require.NoError(t, err, "Subscribe")

	conn.push(t, `{"identifier":`+quote(roomIdentifier)+`,"message":{"body":"Hello!"}}`)

	for _, subscription := range []*Subscription{first, second} {
		assert.Equal(t, `{"body":"Hello!"}`, receive(t, subscription).String(), "expected the broadcast")
	}

	// Only the last subscription standing tells the server to unsubscribe.
	require.NoError(t, first.Unsubscribe(context.Background()), "Unsubscribe")
	conn.expectNoCommand(t)

	require.NoError(t, second.Unsubscribe(context.Background()), "Unsubscribe")
	conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)
}

func TestSubscribeToAConfirmedIdentifierSendsNothing(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)
	subscribed(t, client, conn)

	// Rails has the identifier already and would ignore a second subscribe, so
	// the one confirmation it gave stands for this subscription too.
	connections := make(chan bool, 1)
	_, err := client.Subscribe(context.Background(), room(), OnConnected(func(reconnected bool) { connections <- reconnected }))
	require.NoError(t, err, "Subscribe")

	assert.False(t, <-connections, "a subscription joining a confirmed identifier reported itself as a reconnect")
	conn.expectNoCommand(t)
}

func TestSubscribersJoinAnInFlightSubscribe(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	first := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	second := subscribe(client, room())
	third := subscribe(client, room())
	conn.expectNoCommand(t)

	conn.confirm(t, roomIdentifier)

	for _, subscribing := range []<-chan subscribeResult{first, second, third} {
		require.NoError(t, (<-subscribing).err, "Subscribe")
	}
}

func TestSubscribersJoiningAnInFlightSubscribeShareItsRejection(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	first := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	second := subscribe(client, room())
	conn.expectNoCommand(t)

	conn.reject(t, roomIdentifier)

	require.ErrorIs(t, (<-first).err, ErrRejected)
	require.ErrorIs(t, (<-second).err, ErrRejected)
}

func TestSubscribersJoiningAnInFlightSubscribeFollowItThroughAReconnect(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithBackoff(time.Millisecond, time.Millisecond))
	conn := welcomed(t, client, transport)

	first := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	second := subscribe(client, room())
	conn.expectNoCommand(t)

	conn.Close()

	reconnected := transport.accept(t)
	reconnected.welcome(t)
	reconnected.expectCommand(t, CommandSubscribe, roomIdentifier)
	reconnected.expectNoCommand(t)
	reconnected.confirm(t, roomIdentifier)

	require.NoError(t, (<-first).err, "Subscribe")
	require.NoError(t, (<-second).err, "Subscribe")
}

func TestCancellingTheOnlyInFlightSubscribeTellsTheServer(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	ctx, cancel := context.WithCancel(context.Background())
	subscribing := make(chan error, 1)
	go func() {
		_, err := client.Subscribe(ctx, room())
		subscribing <- err
	}()
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)

	// The server has the subscription whether or not anyone here still wants it,
	// and would ignore the next subscribe for it unless told to let go.
	cancel()
	require.ErrorIs(t, <-subscribing, context.Canceled)
	conn.expectCommand(t, CommandUnsubscribe, roomIdentifier)
	conn.expectNoCommand(t)

	subscribed(t, client, conn)
}

func TestCancellingASubscriberJoiningAnInFlightSubscribeLeavesTheFirstWaiting(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	first := subscribe(client, room())
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)

	ctx, cancel := context.WithCancel(context.Background())
	joining := make(chan error, 1)
	go func() {
		_, err := client.Subscribe(ctx, room())
		joining <- err
	}()
	conn.expectNoCommand(t)

	cancel()
	require.ErrorIs(t, <-joining, context.Canceled)
	conn.expectNoCommand(t)

	conn.confirm(t, roomIdentifier)
	require.NoError(t, (<-first).err, "Subscribe")
}

func TestConnectAfterCloseReportsWhyItStopped(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	welcomed(t, client, transport)

	require.NoError(t, client.Close(), "Close")

	require.ErrorIs(t, client.Connect(context.Background()), ErrClosed)
	transport.refuseDial(t)
}

func TestCloseBeforeConnectLeavesTheClientDead(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)

	require.NoError(t, client.Close(), "Close")

	require.ErrorIs(t, client.Connect(context.Background()), ErrClosed)
	assert.False(t, client.Connected(), "a client closed before it started reports itself connected")
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
	require.NoError(t, (<-subscribing).err, "Subscribe")

	conn.Close()

	select {
	case err := <-closing:
		require.NoError(t, err, "Close")
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
			assert.NoError(t, err, "Subscribe from OnConnected")
		}()
	}))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	require.NoError(t, (<-subscribing).err, "Subscribe")

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

		require.NoError(t, subscription.Unsubscribe(context.Background()), "Unsubscribe")
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
	require.NoError(t, <-connecting, "Connect")

	connections := make(chan bool, 1)
	subscribing := subscribe(client, room(), OnConnected(func(reconnected bool) { connections <- reconnected }))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	require.NoError(t, (<-subscribing).err, "Subscribe")

	assert.False(t, <-connections, "a first connection that took two dials reported itself as a reconnect")
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
	require.ErrorIs(t, subscription.Perform(context.Background(), "speak", nil), ErrNotConnected)
}

func TestRepeatedConfirmationConnectsOnce(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport)
	conn := welcomed(t, client, transport)

	connections := make(chan bool, 2)
	subscribing := subscribe(client, room(), OnConnected(func(reconnected bool) { connections <- reconnected }))
	conn.expectCommand(t, CommandSubscribe, roomIdentifier)
	conn.confirm(t, roomIdentifier)
	require.NoError(t, (<-subscribing).err, "Subscribe")
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
		require.NoError(t, <-connecting, "Connect")

		assert.Equal(t, origin, transport.dialedWith().Header.Get("Origin"), url)
	}
}

func TestExplicitOriginWins(t *testing.T) {
	transport := newFakeTransport()
	client := newTestClient(t, transport, WithOrigin("https://app.example.com"))

	connecting := connect(client)
	transport.accept(t).welcome(t)
	require.NoError(t, <-connecting, "Connect")

	assert.Equal(t, "https://app.example.com", transport.dialedWith().Header.Get("Origin"), "expected the origin given")
}

func TestHeaderIsCopied(t *testing.T) {
	transport := newFakeTransport()
	header := http.Header{"Cookie": {"session=secret"}}
	client := newTestClient(t, transport, WithHeader(header))

	header.Set("Cookie", "session=tampered")

	connecting := connect(client)
	transport.accept(t).welcome(t)
	require.NoError(t, <-connecting, "Connect")

	assert.Equal(t, "session=secret", transport.dialedWith().Header.Get("Cookie"), "expected the header as it was given")
}

func TestEveryDialAsksForTheHeaderAgain(t *testing.T) {
	transport := newFakeTransport()
	transport.failNextDial(errors.New("connection refused"))

	var dials atomic.Int64
	client := newTestClient(t, transport,
		WithBackoff(time.Millisecond, time.Millisecond),
		WithHeader(http.Header{"Origin": {"https://app.example.com"}}),
		WithHeaderFunc(func(context.Context) (http.Header, error) {
			return http.Header{"Authorization": {fmt.Sprintf("Bearer token-%d", dials.Add(1))}}, nil
		}))

	connecting := connect(client)
	transport.accept(t).welcome(t)
	require.NoError(t, <-connecting, "Connect")

	dialed := transport.dialedWith().Header
	assert.Equal(t, "Bearer token-2", dialed.Get("Authorization"), "expected the redial to carry the credentials it asked for then")
	assert.Equal(t, "https://app.example.com", dialed.Get("Origin"), "expected the headers set once to survive")
}

func TestADialIsTurnedDownWhenTheHeaderCannotBeBuilt(t *testing.T) {
	transport := newFakeTransport()

	var asked atomic.Int64
	client := newTestClient(t, transport,
		WithBackoff(time.Millisecond, time.Millisecond),
		WithHeaderFunc(func(context.Context) (http.Header, error) {
			if asked.Add(1) == 1 {
				return nil, errors.New("no credentials to hand over")
			}
			return http.Header{"Authorization": {"Bearer token"}}, nil
		}))

	connecting := connect(client)
	transport.accept(t).welcome(t)
	require.NoError(t, <-connecting, "Connect")

	assert.Equal(t, "Bearer token", transport.dialedWith().Header.Get("Authorization"), "expected the client to dial again after the header failed")
}

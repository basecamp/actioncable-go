package actioncable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestV1JSONSubprotocol(t *testing.T) {
	assert.Equal(t, "actioncable-v1-json", V1JSON{}.Subprotocol())
}

func TestV1JSONEncode(t *testing.T) {
	commands := []struct {
		command Command
		encoded string
	}{
		{
			Command{Name: CommandSubscribe, Identifier: `{"channel":"RoomChannel"}`},
			`{"command":"subscribe","identifier":"{\"channel\":\"RoomChannel\"}"}`,
		},
		{
			Command{Name: CommandUnsubscribe, Identifier: `{"channel":"RoomChannel"}`},
			`{"command":"unsubscribe","identifier":"{\"channel\":\"RoomChannel\"}"}`,
		},
		{
			Command{Name: CommandMessage, Identifier: `{"channel":"RoomChannel"}`, Data: `{"action":"speak"}`},
			`{"command":"message","identifier":"{\"channel\":\"RoomChannel\"}","data":"{\"action\":\"speak\"}"}`,
		},
	}

	for _, expected := range commands {
		encoded, err := V1JSON{}.Encode(expected.command)
		require.NoError(t, err, "encoding %s", expected.command.Name)
		assert.Equal(t, expected.encoded, string(encoded))
	}
}

func TestV1JSONDecode(t *testing.T) {
	frames := []struct {
		payload  string
		expected Incoming
	}{
		{`{"type":"welcome"}`, Incoming{Kind: KindWelcome}},
		{`{"type":"ping","message":1755400000}`, Incoming{Kind: KindPing, Message: Message("1755400000")}},
		{
			`{"type":"disconnect","reason":"server_restart","reconnect":true}`,
			Incoming{Kind: KindDisconnect, Reason: ReasonServerRestart, Reconnect: true},
		},
		{
			`{"type":"confirm_subscription","identifier":"{\"channel\":\"RoomChannel\"}"}`,
			Incoming{Kind: KindConfirmation, Identifier: `{"channel":"RoomChannel"}`},
		},
		{
			`{"type":"reject_subscription","identifier":"{\"channel\":\"RoomChannel\"}"}`,
			Incoming{Kind: KindRejection, Identifier: `{"channel":"RoomChannel"}`},
		},
		{
			`{"identifier":"{\"channel\":\"RoomChannel\"}","message":{"body":"Hello!"}}`,
			Incoming{Kind: KindMessage, Identifier: `{"channel":"RoomChannel"}`, Message: Message(`{"body":"Hello!"}`)},
		},
		{
			`{"type":"something_new","identifier":"x","message":"anything"}`,
			Incoming{Kind: KindMessage, Identifier: "x", Message: Message(`"anything"`)},
		},
	}

	for _, frame := range frames {
		incoming, err := V1JSON{}.Decode([]byte(frame.payload))
		require.NoError(t, err, "decoding %s", frame.payload)
		assert.Equal(t, frame.expected.Kind, incoming.Kind, frame.payload)
		assert.Equal(t, frame.expected.Identifier, incoming.Identifier, frame.payload)
		assert.Equal(t, frame.expected.Message.String(), incoming.Message.String(), frame.payload)
		assert.Equal(t, frame.expected.Reason, incoming.Reason, frame.payload)
		assert.Equal(t, frame.expected.Reconnect, incoming.Reconnect, frame.payload)
	}
}

func TestV1JSONDecodeGarbage(t *testing.T) {
	_, err := (V1JSON{}).Decode([]byte("not json"))
	require.Error(t, err, "expected an error decoding garbage")
}

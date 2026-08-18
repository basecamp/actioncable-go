package actioncable

import "testing"

func TestV1JSONSubprotocol(t *testing.T) {
	if subprotocol := (V1JSON{}).Subprotocol(); subprotocol != "actioncable-v1-json" {
		t.Fatalf("expected actioncable-v1-json, got %q", subprotocol)
	}
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
		if err != nil {
			t.Fatalf("encoding %s: %v", expected.command.Name, err)
		}
		if string(encoded) != expected.encoded {
			t.Fatalf("expected %s, got %s", expected.encoded, encoded)
		}
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
		if err != nil {
			t.Fatalf("decoding %s: %v", frame.payload, err)
		}
		if incoming.Kind != frame.expected.Kind {
			t.Fatalf("expected kind %d for %s, got %d", frame.expected.Kind, frame.payload, incoming.Kind)
		}
		if incoming.Identifier != frame.expected.Identifier {
			t.Fatalf("expected identifier %q for %s, got %q", frame.expected.Identifier, frame.payload, incoming.Identifier)
		}
		if incoming.Message.String() != frame.expected.Message.String() {
			t.Fatalf("expected message %s for %s, got %s", frame.expected.Message, frame.payload, incoming.Message)
		}
		if incoming.Reason != frame.expected.Reason || incoming.Reconnect != frame.expected.Reconnect {
			t.Fatalf("expected %q/%v for %s, got %q/%v", frame.expected.Reason, frame.expected.Reconnect, frame.payload, incoming.Reason, incoming.Reconnect)
		}
	}
}

func TestV1JSONDecodeGarbage(t *testing.T) {
	if _, err := (V1JSON{}).Decode([]byte("not json")); err == nil {
		t.Fatal("expected an error decoding garbage")
	}
}

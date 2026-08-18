package actioncable

import (
	"encoding/json"
	"fmt"
)

// SubprotocolV1JSON is the subprotocol every Rails Action Cable server speaks.
const SubprotocolV1JSON = "actioncable-v1-json"

// V1JSON implements the actioncable-v1-json protocol: JSON objects in text
// frames, keyed by command going out and by type coming in.
type V1JSON struct{}

func (V1JSON) Subprotocol() string {
	return SubprotocolV1JSON
}

func (V1JSON) Encode(command Command) ([]byte, error) {
	return json.Marshal(v1JSONCommand{
		Command:    string(command.Name),
		Identifier: command.Identifier,
		Data:       command.Data,
	})
}

func (V1JSON) Decode(payload []byte) (Incoming, error) {
	var frame v1JSONFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return Incoming{}, fmt.Errorf("actioncable: decoding %s: %w", truncate(payload, 200), err)
	}

	incoming := Incoming{
		Identifier: frame.Identifier,
		Message:    Message(frame.Message),
		Reason:     frame.Reason,
		Reconnect:  frame.Reconnect,
	}

	// Anything without a recognized type is a channel message, which is how the
	// server sends them: an identifier and a message, and no type at all.
	switch frame.Type {
	case "welcome":
		incoming.Kind = KindWelcome
	case "ping":
		incoming.Kind = KindPing
	case "disconnect":
		incoming.Kind = KindDisconnect
	case "confirm_subscription":
		incoming.Kind = KindConfirmation
	case "reject_subscription":
		incoming.Kind = KindRejection
	default:
		incoming.Kind = KindMessage
	}

	return incoming, nil
}

type v1JSONCommand struct {
	Command    string `json:"command"`
	Identifier string `json:"identifier"`
	Data       string `json:"data,omitempty"`
}

type v1JSONFrame struct {
	Type       string          `json:"type"`
	Identifier string          `json:"identifier"`
	Message    json.RawMessage `json:"message"`
	Reason     string          `json:"reason"`
	Reconnect  bool            `json:"reconnect"`
}

func truncate(payload []byte, limit int) string {
	if len(payload) > limit {
		return string(payload[:limit]) + "…"
	} else {
		return string(payload)
	}
}

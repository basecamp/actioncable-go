package actioncable

import "encoding/json"

// A Message is the undecoded payload a channel broadcast or transmitted. Its
// shape is entirely up to the channel, so Unmarshal it into the expected type.
type Message json.RawMessage

func (m Message) Unmarshal(value any) error {
	return json.Unmarshal(m, value)
}

func (m Message) String() string {
	return string(m)
}

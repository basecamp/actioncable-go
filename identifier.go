package actioncable

import (
	"encoding/json"
	"fmt"
)

// Params are the extra attributes that identify a subscription alongside its
// channel name, like the id of the record a channel streams for.
type Params map[string]any

// An Identifier names one subscription. It is encoded as a JSON object and the
// server treats that encoding as an opaque key, echoing it back on every frame
// it sends for the subscription.
//
//	actioncable.Identifier{Channel: "RoomChannel", Params: actioncable.Params{"id": 42}}
//
// A channel with no params needs only the name.
type Identifier struct {
	Channel string
	Params  Params
}

func (i Identifier) String() string {
	if key, err := i.key(); err == nil {
		return key
	} else {
		return fmt.Sprintf("%s(%v)", i.Channel, i.Params)
	}
}

func (i Identifier) key() (string, error) {
	fields := make(map[string]any, len(i.Params)+1)
	for name, value := range i.Params {
		fields[name] = value
	}
	fields["channel"] = i.Channel

	key, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("actioncable: encoding identifier for %q: %w", i.Channel, err)
	}

	return string(key), nil
}

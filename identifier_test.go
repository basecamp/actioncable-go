package actioncable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentifierKey(t *testing.T) {
	identifiers := []struct {
		identifier Identifier
		key        string
	}{
		{Identifier{Channel: "RoomChannel"}, `{"channel":"RoomChannel"}`},
		{Identifier{Channel: "RoomChannel", Params: Params{"id": 42}}, `{"channel":"RoomChannel","id":42}`},
		{
			Identifier{Channel: "RoomChannel", Params: Params{"id": 42, "since": "yesterday"}},
			`{"channel":"RoomChannel","id":42,"since":"yesterday"}`,
		},
	}

	for _, expected := range identifiers {
		key, err := expected.identifier.key()
		require.NoError(t, err, "keying %s", expected.identifier.Channel)
		assert.Equal(t, expected.key, key)
		assert.Equal(t, expected.key, expected.identifier.String(), "String should be the key")
	}
}

func TestIdentifierKeyRefusesParamsItCannotEncode(t *testing.T) {
	identifier := Identifier{Channel: "RoomChannel", Params: Params{"id": func() {}}}

	_, err := identifier.key()
	require.Error(t, err, "expected an error for params that don't encode")
}

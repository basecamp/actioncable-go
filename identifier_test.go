package actioncable

import "testing"

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
		if err != nil {
			t.Fatalf("keying %s: %v", expected.identifier.Channel, err)
		}
		if key != expected.key {
			t.Fatalf("expected %s, got %s", expected.key, key)
		}
		if expected.identifier.String() != expected.key {
			t.Fatalf("expected String to be the key, got %s", expected.identifier)
		}
	}
}

func TestIdentifierKeyRefusesParamsItCannotEncode(t *testing.T) {
	identifier := Identifier{Channel: "RoomChannel", Params: Params{"id": func() {}}}

	if _, err := identifier.key(); err == nil {
		t.Fatal("expected an error for params that don't encode")
	}
}

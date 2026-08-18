package actioncable_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/basecamp/actioncable-go"
)

func Example() {
	client := actioncable.New("wss://example.com/cable")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	room, err := client.Subscribe(ctx, actioncable.Identifier{
		Channel: "RoomChannel",
		Params:  actioncable.Params{"id": 42},
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for message := range room.Messages() {
			var said struct{ Body string }
			if err := message.Unmarshal(&said); err == nil {
				fmt.Println(said.Body)
			}
		}
	}()

	if err := room.Perform(ctx, "speak", map[string]any{"body": "Hello!"}); err != nil {
		log.Fatal(err)
	}
}

// Credentials belong on the client, which sends them while the connection is
// established.
func ExampleNew_authorized() {
	client := actioncable.New("wss://example.com/cable",
		actioncable.WithCookie("_session_id=1234"),
		actioncable.WithHeader(http.Header{"X-Api-Token": {"secret"}}),
	)
	defer client.Close()

	if err := client.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// A subscription outlives reconnects, so OnConnected reports whether this is a
// fresh subscription or one that came back and may have missed messages.
func ExampleClient_Subscribe() {
	client := actioncable.New("wss://example.com/cable")

	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	room, err := client.Subscribe(ctx, actioncable.Identifier{Channel: "RoomChannel"},
		actioncable.OnConnected(func(reconnected bool) {
			if reconnected {
				catchUp()
			}
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	for message := range room.Messages() {
		fmt.Println(message)
	}
}

func catchUp() {}

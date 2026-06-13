package main

import (
	"encoding/json"
	"log"

	"github.com/hellinvoid/dist-sys-flyio/internal/snowflake"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
	Snowflake method
	timestamp-node_id-counter

	Cons:
	!!	Clock synchronization
*/

func main() {
	n := maelstrom.NewNode()
	idgen := snowflake.NewIdGenerator()

	n.Handle("generate", func(msg maelstrom.Message) error {

		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = idgen.GetUniqueId(n.ID())

		return n.Reply(msg, body)
	})

	log.Println("Server running...")
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

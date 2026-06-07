package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
	Simplest valid approach
	node_id-{ATOMIC_counter}
	node_id can be anything unique to the node
	in this case msg.Dest
*/

func main() {
	n := maelstrom.NewNode()
	var counter atomic.Uint64
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"

		body["id"] = fmt.Sprintf("%s-%d", msg.Dest, counter.Add(1))

		return n.Reply(msg, body)
	})

	log.Println("Server running...")
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

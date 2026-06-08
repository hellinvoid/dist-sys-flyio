package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	arr := make([]float64, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		arr = append(arr, body["message"].(float64))
		mu.Unlock()

		body["type"] = "broadcast_ok"

		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = slices.Clone(arr)

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"

		delete(body, "topology")
		return n.Reply(msg, body)
	})

	log.Println("Server running...")
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

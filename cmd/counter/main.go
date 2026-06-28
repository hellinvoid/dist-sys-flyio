package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		// Check existence of key and write 0 if key does not exist
		_, err := kv.Read(ctx, n.ID())
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			kv.CompareAndSwap(ctx, n.ID(), 0, 0, true)
		} else if err != nil {
			return err
		}

		// Loop over and add delta untill success
		for {
			v, err := kv.ReadInt(ctx, n.ID())
			if err != nil {
				return err
			}

			err = kv.CompareAndSwap(ctx, n.ID(), v, v+delta, false)
			if err == nil {
				break
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		ids := n.NodeIDs()
		sum := 0
		for _, id := range ids {
			v, _ := kv.ReadInt(ctx, id)
			sum += v
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": sum,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

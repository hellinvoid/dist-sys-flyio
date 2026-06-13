package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/hellinvoid/dist-sys-flyio/internal/snowflake"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	idgen := snowflake.NewIdGenerator()

	var once sync.Once

	var mu sync.Mutex
	arr := make([]float64, 0)

	n := maelstrom.NewNode()
	next := make([]string, 0)

	seenGossipUid := map[float64]any{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := body["message"].(float64)

		mu.Lock()
		arr = append(arr, val)
		mu.Unlock()

		go SendNewGossip(n, next, val, idgen)

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
		mu.Lock()
		msgs := append([]float64(nil), arr...)
		mu.Unlock()

		body["messages"] = msgs

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		isMsgFromNode := msg.Src[0] == 'n'

		once.Do(func() {
			mu.Lock()

			neighbours := body["topology"].(map[string]any)[n.ID()].([]any)
			for _, n := range neighbours {
				next = append(next, n.(string))
			}

			for _, nxt := range next {
				if msg.Src != nxt {
					n.Send(nxt, body)
				}
			}
			for _, val := range arr {
				go SendNewGossip(n, next, val, idgen)
			}

			mu.Unlock()
		})

		body["type"] = "topology_ok"

		delete(body, "topology")

		if isMsgFromNode {
			return nil
		}

		return n.Reply(msg, body)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		gVal := body["gossip_val"].(float64)
		gUid := body["gossip_uid"].(float64)

		mu.Lock()
		if _, ok := seenGossipUid[gUid]; ok {
			mu.Unlock()
			return nil
		}

		seenGossipUid[gUid] = struct{}{}
		arr = append(arr, gVal)
		mu.Unlock()

		for _, nxt := range next {
			n.Send(nxt, body)
		}

		return nil
	})

	log.Println("Server running...")
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func SendNewGossip(n *maelstrom.Node, next []string, val float64, idgen *snowflake.IdGenerator) {
	gossipBody := make(map[string]any)
	gossipBody["type"] = "gossip"
	gossipBody["gossip_uid"] = idgen.GetUniqueId(n.ID())
	gossipBody["gossip_val"] = val
	for _, nxt := range next {
		n.Send(nxt, gossipBody)
	}
}

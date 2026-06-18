package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hellinvoid/dist-sys-flyio/internal/snowflake"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	POLL_INTERVAL = 100 * time.Millisecond
	POLL_TIMEOUT  = 700 * time.Millisecond
)

type Entry struct {
	UId uint64
	Val float64
}

func main() {

	n := maelstrom.NewNode()

	idgen := snowflake.NewIdGenerator()

	var once sync.Once
	var mu sync.Mutex
	entries := make([]Entry, 0)

	var seenUid sync.Map

	startPoll := func(n *maelstrom.Node, nxt string) {
		body := make(map[string]any)
		body["type"] = "poll"

		var offset atomic.Int64

		pollResponseHandler := func(msg maelstrom.Message) error {
			var res map[string]any
			if err := json.Unmarshal(msg.Body, &res); err != nil {
				return err
			}

			arr := res["entries"].([]any)
			offset.Add(int64(len(arr)))

			for _, obj := range arr {
				eMap := obj.(map[string]any)
				e := Entry{
					UId: uint64(eMap["UId"].(float64)),
					Val: eMap["Val"].(float64),
				}

				if _, ok := seenUid.LoadOrStore(e.UId, struct{}{}); !ok {
					addEntry(&entries, e, &mu)
				}

			}

			return nil
		}

		ticker := time.NewTicker(POLL_INTERVAL)

		for range ticker.C {
			body["offset"] = offset.Load()
			ctx, cancel := context.WithTimeout(context.Background(), POLL_TIMEOUT)
			msg, err := n.SyncRPC(ctx, nxt, body)
			cancel()
			if err != nil {
				continue
			}
			pollResponseHandler(msg)
		}

	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		go n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})

		val := body["message"].(float64)

		e := Entry{
			UId: idgen.GetUniqueId(n.ID()),
			Val: val,
		}

		seenUid.Store(e.UId, struct{}{})
		addEntry(&entries, e, &mu)

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"

		msgs := make([]float64, 0)

		mu.Lock()
		for _, e := range entries {
			msgs = append(msgs, e.Val)
		}
		mu.Unlock()

		body["messages"] = msgs

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if msg.Src[0] != 'n' {
			go n.Reply(msg, map[string]any{
				"type": "topology_ok",
			})
		}

		neighbours := body["topology"].(map[string]any)[n.ID()].([]any)
		once.Do(func() {
			for _, neigh := range neighbours {
				nxt := neigh.(string)
				n.Send(nxt, body)
				go startPoll(n, nxt)
			}
		})

		return nil
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "poll_ok"

		offset := int(body["offset"].(float64))

		if offset < entriesLen(&entries, &mu) {
			body["entries"] = entries[offset:]
		} else {
			body["entries"] = []Entry{}
		}

		return n.Reply(msg, body)
	})

	log.Println("Server running...")
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func addEntry(arr *[]Entry, e Entry, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()

	*arr = append(*arr, e)
}

func entriesLen(arr *[]Entry, mu *sync.Mutex) int {
	mu.Lock()
	defer mu.Unlock()
	return len(*arr)
}

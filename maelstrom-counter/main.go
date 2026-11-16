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
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			current, err := kv.ReadInt(ctx, n.ID())
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				current = 0
			} else if err != nil {
				continue
			}

			err = kv.CompareAndSwap(ctx, n.ID(), current, current+body.Delta, true)
			if err == nil {
				break
			}
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		sum := 0
		for _, id := range n.NodeIDs() {
			for {
				value, err := kv.ReadInt(ctx, id)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					break
				}
				if err != nil {
					continue
				}
				sum += value
				break
			}
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
package main

import (
	"encoding/json"
	"log"
	"sync"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	messages := map[int]struct{}{}
	var neighbors []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
			MsgID int `json:"msg_id"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		_, seen := messages[body.Message]

		if !seen {
			messages[body.Message] = struct{}{}
		}
		dests := append([]string(nil), neighbors...)
		mu.Unlock()

		if !seen {
			for _, dest := range dests {
				if err := n.Send(dest, map[string]any {
					"type": "broadcast",
					"message": body.Message,
				}); err != nil {
					return err
				}
			}
		}

		if body.MsgID == 0 {
			return nil
		}


		return n.Reply(msg, map[string]any {
			"type": "broadcast_ok",
		})

	})
	
	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		seen := make([]int, 0, len(messages))
		for m := range messages {
			seen = append(seen, m)
		}
		mu.Unlock()

		return n.Reply(msg, map[string] any {
			"type": "read_ok",
			"messages": seen,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		neighbors = body.Topology[n.ID()]
		mu.Unlock()

		return n.Reply(msg, map[string]any {
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
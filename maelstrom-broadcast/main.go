package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func replicate(n *maelstrom.Node, dest string, message int) {
	body := map[string]any {
		"type": "broadcast",
		"message": message,
	}
	for {
		ack := make(chan struct{})
		var once sync.Once

		err := n.RPC(dest, body, func(maelstrom.Message) error {
			once.Do(func() { close(ack)})
			return nil
		})

		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		select {
			case <-ack:
				return
			case <-time.After(time.Second):
				continue
		}
	}
}

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
				go replicate(n, dest, body.Message)
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
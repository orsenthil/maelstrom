package main

import (
	"encoding/json"
	"sync"
	"log"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var mu sync.Mutex
	store := map[int]int{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			Txn [][]any `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		fromClient := strings.HasPrefix(msg.Src, "c")
	
		mu.Lock()
		for _, op := range body.Txn {
			key := int(op[1].(float64))
			switch op[0] {
			case "r":
				if v, ok := store[key]; ok {
					op[2] = v
				}
			case "w":
				store[key] = int(op[2].(float64))
			}
		}
		mu.Unlock()

		if !fromClient {
			return nil
		}
		
		if err := n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  body.Txn,
		}); err != nil {
			return err
		}
		
		var writes [][]any
		for _, op := range body.Txn {
			if op[0] == "w" {
				writes = append(writes, op)
			}
		}
		for _, id := range n.NodeIDs() {
			if id == n.ID() || len(writes) == 0 {
				continue
			}
			n.Send(id, map[string]any{
				"type": "txn",
				"txn":  writes,
			})
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
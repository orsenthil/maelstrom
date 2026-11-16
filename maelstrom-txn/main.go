package main

import (
	"encoding/json"
	"sync"
	"log"
	"context"
	"time"


	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type version struct {
	lamport int
	node    string
	value   int
}

func newer(haveLamport, txnLamport int, haveNode, txnNode string) bool {
	if haveLamport != txnLamport {
		return haveLamport > txnLamport
	}
	return haveNode > txnNode
}

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	var lamport int
	store := map[int]version{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			Txn [][]any `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		tsLamport := lamport
		tsNode := n.ID()
		lamport++
		for _, op := range body.Txn {
			key := int(op[1].(float64))
			switch op[0] {
			case "r":
				if cur, ok := store[key]; ok {
					op[2] = cur.value
				}
			case "w":
				value := int(op[2].(float64))
				cur, ok := store[key]
				if !ok || !newer(cur.lamport, tsLamport, cur.node, tsNode) {
					store[key] = version{tsLamport, tsNode, value}
				}
			}
		}
		mu.Unlock()
		
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
			go func(dest string) {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, err := n.SyncRPC(ctx, dest, map[string]any{
						"type":    "replicate",
						"txn":     writes,
						"lamport": tsLamport,
						"node":    tsNode,
					})
					cancel()
					if err == nil {
						return
					}
				}
			}(id)
		}
		return nil
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body struct {
			Txn     [][]any `json:"txn"`
			Lamport int     `json:"lamport"`
			Node    string  `json:"node"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
	
		mu.Lock()
		if body.Lamport+1 > lamport {
			lamport = body.Lamport + 1
		}
		for _, op := range body.Txn {
			key := int(op[1].(float64))
			value := int(op[2].(float64))
			cur, ok := store[key]
			if !ok || !newer(cur.lamport, body.Lamport, cur.node, body.Node) {
				store[key] = version{body.Lamport, body.Node, value}
			}
		}
		mu.Unlock()
	
		return n.Reply(msg, map[string]any{"type": "replicate_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
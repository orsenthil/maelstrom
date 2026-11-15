package main

import (
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var seq atomic.Int64

	n.Handle("generate", func(msg maelstrom.Message) error {
		id := fmt.Sprintf("%s-%d", n.ID(), seq.Add(1))
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
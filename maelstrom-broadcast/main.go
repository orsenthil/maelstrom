package main

import (
	"encoding/json"
	"log"
	"sync"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sort"
)

func treeNeighbors(n *maelstrom.Node) [] string {
	ids := append([]string(nil), n.NodeIDs()...)
	sort.Strings(ids)

	i := 0
	for i < len(ids) && ids[i] != n.ID() {
		i++
	}
	var out []string
	if i > 0 {
		out = append(out, ids[(i-1) / 4])
	}
	for child := 4 * i + 1; child <= 4 * i + 4 && child < len(ids); child++ {
		out = append(out, ids[child])
	}
	return out
}


func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	var neighbors []string
	var once sync.Once
	messages := map[int]struct{}{}

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
		mu.Unlock()

		once.Do(func() {
			neighbors = treeNeighbors(n)
		})

		if !seen {
			for _, dest := range neighbors {
				if dest  == msg.Src {
					continue
				}
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

		return n.Reply(msg, map[string]any {
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
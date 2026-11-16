package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type entry struct {
	offset int
	msg    int
}

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	logs := map[string][]entry{}
	commits := map[string]int{}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		offset := len(logs[body.Key])
		logs[body.Key] = append(logs[body.Key], entry{offset, body.Msg})
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		msgs := map[string][][2]int{}
		for key, from := range body.Offsets {
			for _, e := range logs[key] {
				if e.offset >= from {
					msgs[key] = append(msgs[key], [2]int{e.offset, e.msg})
				}
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for key, off := range body.Offsets {
			if prev, ok := commits[key]; !ok || off > prev {
				commits[key] = off
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		offsets := map[string]int{}
		for _, key := range body.Keys {
			if off, ok := commits[key]; ok {
				offsets[key] = off
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
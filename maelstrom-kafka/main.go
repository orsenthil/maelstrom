package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type entry struct {
	offset int
	msg    int
}

func appendLog(ctx context.Context, kv *maelstrom.KV, key string, msg int) int {
	for {
		var log [][2]int
		err := kv.ReadInto(ctx, "log:"+key, &log)
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			log = nil
		} else if err != nil {
			continue
		}

		offset := len(log)
		next := make([][2]int, offset+1)
		copy(next, log)
		next[offset] = [2]int{offset, msg}

		err = kv.CompareAndSwap(ctx, "log:"+key, log, next, true)
		if err == nil {
			return offset
		}
	}
}

func commitOffset(ctx context.Context, kv *maelstrom.KV, key string, off int) {
	for {
		current, err := kv.ReadInt(ctx, "commit:"+key)
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			current = -1
		} else if err != nil {
			continue
		}
		if off <= current {
			return
		}
		if kv.CompareAndSwap(ctx, "commit:"+key, current, off, true) == nil {
			return
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ctx := context.Background()

	var mu sync.Mutex

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		offset := appendLog(ctx, kv, body.Key, body.Msg)
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
			var entries [][2]int
			for {
				err := kv.ReadInto(ctx, "log:"+key, &entries)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					entries = nil
					break
				}
				if err == nil {
					break
				}
			}
			for _, pair := range entries {
				if pair[0] >= from {
					msgs[key] = append(msgs[key], pair)
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
			commitOffset(ctx, kv, key, off)
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
			for {
				off, err := kv.ReadInt(ctx, "commit:"+key)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					break
				}
				if err != nil {
					continue
				}
				offsets[key] = off
				break
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
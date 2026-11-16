package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sort"

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

func owner(n *maelstrom.Node, key string) string {
	ids := append([]string(nil), n.NodeIDs()...)
	sort.Strings(ids)
	h := 0
	for _, c := range key {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return ids[h%len(ids)]
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

	var cacheMu sync.Mutex
	cache := map[string][][2]int{}
	commits := map[string]int{}

	var keyMu sync.Mutex
	keyLocks := map[string]*sync.Mutex{}
	lockKey := func(key string) *sync.Mutex {
		keyMu.Lock()
		defer keyMu.Unlock()
		m, ok := keyLocks[key]
		if !ok {
			m = &sync.Mutex{}
			keyLocks[key] = m
		}
		return m
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if dest := owner(n, body.Key); dest != n.ID() {
			var respBody struct {
				Offset int `json:"offset"`
			}
			for {
				resp, err := n.SyncRPC(ctx, dest, map[string]any{
					"type": "send",
					"key":  body.Key,
					"msg":  body.Msg,
				})
				if err != nil {
					continue
				}
				if err := json.Unmarshal(resp.Body, &respBody); err != nil {
					return err
				}
				return n.Reply(msg, map[string]any{
					"type":   "send_ok",
					"offset": respBody.Offset,
				})
			}
		}
		
		lk := lockKey(body.Key)
		lk.Lock()
		offset := appendLog(ctx, kv, body.Key, body.Msg)
		lk.Unlock()
		
		cacheMu.Lock()
		cache[body.Key] = append(cache[body.Key], [2]int{offset, body.Msg})
		cacheMu.Unlock()
		
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

		msgs := map[string][][2]int{}

		for key, from := range body.Offsets {
			cacheMu.Lock()
			entries := cache[key]
			var suffix [][2]int
			covers := len(entries) > 0 && entries[len(entries)-1][0] >= from
			if owner(n, key) == n.ID() || covers {
				for _, pair := range entries {
					if pair[0] >= from {
						suffix = append(suffix, pair)
					}
				}
				cacheMu.Unlock()
				if len(suffix) > 0 {
					msgs[key] = suffix
				}
				continue
			}
			cacheMu.Unlock()
		
			var fresh [][2]int
			for {
				err := kv.ReadInto(ctx, "log:"+key, &fresh)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					fresh = nil
					break
				}
				if err == nil {
					break
				}
			}
			cacheMu.Lock()
			cache[key] = fresh
			cacheMu.Unlock()
			for _, pair := range fresh {
				if pair[0] >= from {
					msgs[key] = append(msgs[key], pair)
				}
			}
		}


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

		for key, off := range body.Offsets {
			commitOffset(ctx, kv, key, off)
			cacheMu.Lock()
			if prev, ok := commits[key]; !ok || off > prev {
				commits[key] = off
			}
			cacheMu.Unlock()
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := map[string]int{}

		for _, key := range body.Keys {

			cacheMu.Lock()
			if off, ok := commits[key]; ok {
				offsets[key] = off
				cacheMu.Unlock()
				continue
			}
			cacheMu.Unlock()

			for {
				off, err := kv.ReadInt(ctx, "commit:"+key)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					break
				}
				if err != nil {
					continue
				}

				cacheMu.Lock()
				commits[key] = off
				cacheMu.Unlock()
				offsets[key] = off
				break
			}
		}

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
# Distributed Systems Challenges from Fly.io

* https://fly.io/dist-sys/

Every challenge is the same kind of program. A node reads a JSON message, updates state, and writes a JSON message. Each one adds a constraint: identity, memory, dissemination, faults, cost, a shared total order, then transactions that stay available while the network is cut.

## A node is a participant, not a server you call

Maelstrom starts your binary once per node. Messages arrive on stdin and leave on stdout. `src` and `dest` name the sender and the receiver. Clients are `c1`, `c2`, and nodes are `n0`, `n1`. The body has a `type`, and `n.Handle` selects a function the way an HTTP router selects a handler.

Echo is only this. It changes `"echo"` to `"echo_ok"` and sends the body back. Every later challenge is that loop with state added.

```go
body["type"] = "echo_ok"
return n.Reply(msg, body)
```

`n.Run` is the loop. It reads stdin and dispatches each message on its own goroutine. `main` has to call it and stay there. Returning after `n.Handle` exits the process with status 0, and Maelstrom reports that as a crash even though stderr is empty.

```go
if err := n.Run(); err != nil {
    log.Fatal(err)
}
```

## A reply is tied to one request

`msg_id` identifies a request. `n.Reply` sets `in_reply_to` to that id, so the sender knows which call completed. Echo uses this and never thinks about it.

Broadcast uses the same field as a signal. A client request has a `msg_id` and must get `broadcast_ok`. A gossip message sent with `n.Send` has none, and answering it would produce a `broadcast_ok` that nobody is waiting for. `n.RPC` puts a `msg_id` on an inter-node message on purpose, and the callback runs when the matching reply arrives. That is how a node tells a retry to stop.

```go
if body.MsgID == 0 {
    return nil
}
return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
```

## Decide locally when the network may be cut

Unique ids run while Maelstrom partitions the cluster. A node that asks the others for the next id will stall for the whole cut. `n.ID()` is already unique to this process, and an atomic counter is unique inside it, so `n0-1` and `n1-1` cannot collide. The atomic add matters because handlers run concurrently and two of them must not take the same number.

```go
id := fmt.Sprintf("%s-%d", n.ID(), seq.Add(1))
```

Broadcast keeps the same rule for reads. `read` returns the integers stored on this node. It does not ask a peer what it has seen.

```go
return n.Reply(msg, map[string]any{
    "type":     "read_ok",
    "messages": seen, // copied from this node's map
})
```

## Shared memory inside one node still needs a lock

Handlers for one node run at the same time. The id counter is one integer, so `atomic.Int64` is enough. The broadcast set is a map, so a mutex covers the check and the insert together. Two handlers can otherwise both observe "not seen" and both forward the same integer.

Go has no set type. `map[int]struct{}` is a set: the key is the integer, and `struct{}` is a value that takes no memory. A second insert of the same key changes nothing.

```go
mu.Lock()
_, seen := messages[body.Message]
if !seen {
    messages[body.Message] = struct{}{}
}
mu.Unlock()
```

A map variable with no composite literal is nil. Reading it returns "missing," so a transaction of only reads looks fine. The first write panics with `assignment to entry in nil map`. Create it at the declaration.

```go
store := map[int]version{}
```

## Delivering a value is gossip plus a rule for stopping

On one node, broadcast is just the set. `read` lists the keys. `topology` introduces neighbors and, at this stage, only needs `topology_ok`.

With several nodes, the node that learns a new integer tells its neighbors, and they tell theirs. That path includes the node that just sent it, so the same integer circulates forever. The set is the stop rule. Forward only when the insert is new. A later copy is ignored.

Who you tell is the topology, and it is a choice.

Neighbors from Maelstrom form a grid. A value crosses several hops, and some links carry duplicates. Skipping the sender cuts the copies that would otherwise bounce straight back.

```go
if !seen {
    for _, dest := range neighbors {
        if dest == msg.Src {
            continue
        }
        n.Send(dest, map[string]any{"type": "broadcast", "message": body.Message})
    }
}
```

Fan-out means the node that took the client request sends straight to every other node. Peers store the value and acknowledge it. They do not relay, because their `src` starts with `n` rather than `c`. One hop, and the message count is one request plus one reply per peer.

```go
if !seen && strings.HasPrefix(msg.Src, "c") {
    for _, id := range n.NodeIDs() {
        if id != n.ID() {
            go replicate(n, id, body.Message)
        }
    }
}
```

A tree means each node talks only to a parent and a few children. Every node forwards, including one that heard the value from a peer. A tree of 25 nodes has 24 links, so each broadcast crosses the cluster once. Sorting the ids makes every node compute the same parent and children.

```go
if i > 0 {
    out = append(out, ids[(i-1)/4]) // parent
}
for child := 4*i + 1; child <= 4*i+4 && child < len(ids); child++ {
    out = append(out, ids[child])
}
```

## A send is not a delivery

`n.Send` writes the message and moves on. If a partition drops it, the sender never finds out. `n.RPC` waits for `broadcast_ok`. Until that reply arrives, the sender tries again about once a second.

```go
err := n.RPC(dest, body, func(maelstrom.Message) error {
    once.Do(func() { close(ack) })
    return nil
})
select {
case <-ack:
    return
case <-time.After(time.Second):
}
```

The retry is a goroutine. The client's own `broadcast_ok` goes out immediately. Holding that reply until every peer answers would make the client wait out the partition.

Retries are safe only because the receiver is idempotent. It already stored the integer, so it does not forward again, and it still replies. The reply is what ends the retry.

An acknowledgement costs a message. The last broadcast budget is too small for one on every hop, and that test does not cut the network, so the tree goes back to `n.Send`.

## Latency and message count move against each other

Maelstrom delays each hop, and it counts messages between nodes per operation. Reads are about half the operations and send nothing to other nodes, so they cut the average in half.

A grid is several hops with extra copies. Fan-out is one hop and about two messages per peer. A 4-child tree is a handful of hops and one message per link. The two loops above are the whole tradeoff: `n.NodeIDs()` spends messages to buy one hop, and `treeNeighbors` spends hops to buy a message count near 12.

## A counter is one number per node

The grow-only counter has to stay available during a partition, and the final read on each node has to land in the checker's range: every `add` that returned `add_ok`, plus any subset of the adds whose result was lost. An add that returned an error is definitely absent, so the handler keeps retrying a compare-and-swap rather than failing after a write that may have landed.

`seq-kv` is enough because each node owns one key, its own id. A missing key is 0. The node reads its key, swaps in `current + delta`, and repeats until the swap matches. `createIfNotExists` makes the first swap create the key. `read` sums `ReadInt` across `n.NodeIDs()`.

```go
err = kv.CompareAndSwap(ctx, n.ID(), current, current+body.Delta, true)
if err == nil {
    break
}
```

## An offset is the length of the log

On one node the Kafka log is a slice per key. The offset of a new record is `len(log)` before the append, so offsets start at 0 and stay contiguous. `poll` returns pairs with `offset >= from`, in order, and omits a key that has nothing. A commit only moves forward, including the first commit of 0. `list_committed_offsets` omits a key that was never committed.

`seq-kv` is the wrong store once two nodes assign offsets. It can compare-and-swap against a stale read, so two nodes can both believe they own offset 3. `lin-kv` puts every compare-and-swap in one total order. The value is the whole log. Copy it before appending, or `from` and `to` alias the same slice and the swap matches a value you already changed. A missing commit is `-1`, because 0 is a real offset and must be stored. `send_ok` goes out only after the swap succeeds.

```go
offset := len(log)
next := make([][2]int, offset+1)
copy(next, log)
next[offset] = [2]int{offset, msg}
err = kv.CompareAndSwap(ctx, "log:"+key, log, next, true)
```

## One writer per key, and a memory of what you already read

A mutex around the `lin-kv` calls orders handlers on this node only. The other node still swaps the same key, and every poll waits behind every send. The efficient log keeps two cuts.

The first is a cache. After a successful append, store the pair. A poll whose cached last offset is already `>= from` copies that suffix and skips `lin-kv`. The cache is a prefix of the log, so answering from it cannot skip a record. Commits follow the same rule: remember the highest offset this node has seen, and read `lin-kv` only on a miss. A slightly older commit is legal.

The second cut is an owner. Sort `n.NodeIDs()` and send each key to `ids[hash(key) % n]`. The node that does not own the key forwards the same `send` with `SyncRPC` and returns that offset. Only the owner calls `appendLog`. A per-key mutex covers that call, so two local clients of the owner do not swap each other. It stays off the forward path, or a poll on the owner would wait on another node's network.

```go
if dest := owner(n, body.Key); dest != n.ID() {
    resp, err := n.SyncRPC(ctx, dest, map[string]any{
        "type": "send",
        "key":  body.Key,
        "msg":  body.Msg,
    })
}
```

## A transaction is the list, applied in order

A `txn` message is a list of `["r", key, null]` and `["w", key, value]`. The reply is `txn_ok` with that same list, and each read's third element filled in. A missing key stays `null`. JSON numbers arrive as `float64`, so the key and the written value are converted with `int(...)`.

On one node a mutex around the whole list is the transaction. A later read in the same list sees a write earlier in the list, because the map is updated as the loop goes. Two handlers otherwise interleave operations from different transactions.

```go
key := int(op[1].(float64))
switch op[0] {
case "r":
    if v, ok := store[key]; ok {
        op[2] = v
    }
case "w":
    store[key] = int(op[2].(float64))
}
```

## Gossip the writes after the client has its answer

With two nodes, the node that received the client transaction applies it, replies, and then `n.Send`s the writes to the others. The reply stays ahead of the send so a partition cannot hold the client. A message whose `src` starts with `n` applies those writes and returns. It does not reply, and it does not forward. A reply to `n.Send` is a `txn_ok` nobody is waiting for, and `n.Run` stops. Forwarding the same transaction again circulates it forever.

Read uncommitted allows this. A node may serve a write the other node has not seen yet. The checker also does not flag write cycles, so the two nodes may apply the same keys in different orders.

```go
fromClient := strings.HasPrefix(msg.Src, "c")
if !fromClient {
    return nil
}
```

## The later stamp is the one that stays

Read committed adds three rules on top of that. A committed transaction must not read a write from a transaction that then aborted. It must not read an intermediate write when the writer stored a later value for the same key. And two committed transactions must not each read the other's write.

Aborting with `maelstrom.TxnConflict` is optional. This node never aborts. It commits locally, under the lock, before any other transaction can read, and it replicates the whole batch with one stamp. A reader sees the last write of that transaction.

The stamp is a Lamport clock plus the node id. `(1, "n0")` is after `(0, "n1")`. The transaction takes the clock value this node has already reached, then increments. A replicated batch moves the receiver's clock past the sender's stamp before the writes are applied. A write replaces the stored value only when the stored stamp is not already newer. The same stamp applied twice stores the same value, so the retry is safe. A delayed copy of an older transaction leaves the newer value in place.

That ordering is what breaks the cycle. A transaction that already applied the other node's write holds a later stamp. The other transaction finished at an earlier stamp, so it cannot also have read this one's write.

The client still receives `txn_ok` before the other node answers. A goroutine calls `SyncRPC` with a one-second timeout and repeats until `replicate_ok`. During a partition the call times out and the client is already done.

```go
tsLamport := lamport
tsNode := n.ID()
lamport++
if !ok || !newer(cur.lamport, tsLamport, cur.node, tsNode) {
    store[key] = version{tsLamport, tsNode, value}
}
```

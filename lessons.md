# Concepts across echo and broadcast

Echo, unique ids, and broadcast are the same kind of program. A node reads a JSON message, updates local state, and writes a JSON message. Each later challenge reuses that and adds one distributed-systems constraint: identity, memory, dissemination, faults, then cost.

## A node is a participant, not a server you call

Maelstrom starts your binary once per node. Messages arrive on stdin and leave on stdout. `src` and `dest` name the sender and the receiver. Clients are `c1`, `c2`, and nodes are `n0`, `n1`. The body has a `type`, and `n.Handle` selects a function the way an HTTP router selects a handler.

Echo is only this. It changes `"echo"` to `"echo_ok"` and sends the body back. Every later challenge is that loop with state added.

```go
body["type"] = "echo_ok"
return n.Reply(msg, body)
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

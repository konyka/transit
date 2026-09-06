# Raft queue log

When a broker has a Raft instance (`t_broker_set_raft`), durable queue
mutations go through the Raft log. The log is the WAL: apply does not
open a per-queue WAL. Unclustered durable queues keep the existing
local WAL.

## Fail closed

- `POST` / `t_broker_publish` on a Raft-attached leader appends a `PUT`
  command and returns success only after majority commit **and** apply.
- `CONFIRM` / `t_broker_ack` appends an `ACK` command the same way.
- A follower client `POST` still returns `T_ERR_AGAIN` (leader hint).
  `t_client_open_follow` redials that hint once when it names another
  client port.
- A leader that cannot gather a majority returns an error; the message
  is not visible in the queue. The uncommitted entry stays in the log
  (a later successful propose in the same term may commit it). Client
  retry after an error is at-least-once.
- Cluster attached **without** Raft is unchanged: immediate local
  publish, leader check only.

Single-node membership (`cluster_n == 1`) is a majority of one: the
leader commits and applies immediately after append.

## Commands

Compact big-endian payloads, exact length. Queue names use the client
wire charset (`[A-Za-z0-9._-]`).

`PUT` (`type = 1`):

```
u8  type
u8  qtype
u8  qflags
u8  priority
u64 msg_id
u16 name_len
u8  name[name_len]
u32 data_len
u8  data[data_len]
```

`msg_id` is the Raft log index so every node restores the same id
(`t_queue_restore`). Apply creates the queue if it is missing.

`CREATE` (`type = 3`):

```
u8  type
u8  qtype
u8  qflags
u16 name_len
u8  name[name_len]
```

Leader `OPEN` of a missing queue proposes `CREATE` and waits for majority
apply. Followers refuse `OPEN` of an unknown queue with `T_ERR_AGAIN`.

`DELETE` (`type = 4`):

```
u8  type
u16 name_len
u8  name[name_len]
```

`t_broker_delete_queue` (including autodelete) proposes `DELETE` when Raft
is attached.

`ACK` (`type = 2`):

```
u8  type
u64 msg_id
u16 name_len
u8  name[name_len]
```

Apply calls `t_queue_drop` so the id is removed from pending (follower)
or inflight (leader that already `PUSH`ed).

`REJECT` / nack stays local. An unacked message is still in the
follower pending set and can be redelivered after failover.

## Commit

`t_raft_majority_commit` counts self plus peer `match_index` values.
It only commits an index whose entry term equals `current_term`
(Raft Figure 8). Older entries commit together with that index.

`t_peer` records `match_index` from `AppendResp`, then majority-commits
and applies. Inbound `AppendReq` already advances `commit_index`;
`t_raft_rpc` then applies so followers materialize queue state.

Heartbeats are incremental: each peer is sent entries after its
`match_index` with a matching `prev_log_index` / `prev_log_term`.

A leader append can invoke `t_raft_replicate` (set by `t_peer`) so a
client `POST` waits for a blocking peer RPC before the ACK. After
majority commit the leader sends a second AppendRPC with the new
`leader_commit` so followers apply before the client is ACKed.

## Durable header

`t_raft_open_log` writes magic `TRFT`. Version 2 is a 32-byte header:
term, `votedFor`, and `commit_index`. Version 1 files (24-byte header)
still open; they are rewritten as version 2. Restart loads `raft.log.snap`
if present (`last_applied` starts at the snapshot index), then replays
only the log tail.

`transit-server -C` with `-d` stores `datadir/raft.log` and applies it
before campaigning.

## Snapshot

After apply, a node may write `raft.log.snap` (magic `TRFS`) with the
broker's live queues and pending/inflight messages, then drop log
entries through `last_applied`. Restart loads the snapshot first
(`last_applied` starts at the snapshot index), then only the tail.

Compact is fail-closed: it runs only when the in-memory log is at least
`compact_min` entries (default 64) **and** every other voting member's
`match_index` is at least `last_applied`. A peer that is behind keeps
the prefix. A lagging or wiped peer is caught up with `InstallSnapshot`
(`T_WIRE_CLUSTER_SNAP_REQ` / `RESP`): the leader sends the snapshot
bytes, the follower replaces queue state, and `match_index` advances
to `last_included_index`. A corrupt snapshot is rejected.

The snapshot payload is big-endian: `u32` domain count, then each
domain (`u16` name, `u32` queues) and each queue (`qtype`, `qflags`,
name, `u32` messages of `id`/`priority`/`data`). Inflight is stored
as pending so failover redelivers.

## Membership

Static list only (no joint consensus). `transit-server -n <id>` /
`[cluster] id=` and `[cluster] peers=id@host:peer[/client],...`.
`peer` is the Raft listen port. Optional `/client` is the client-port
hint used in `T_ERR_AGAIN` ACKs (underscore form `host_client`). Without
`/client` the hint is omitted — the peer port is never advertised as a
client address. Self is always added with the bound peer address and the
local client listen port. A listed self peer port must match `-C` /
`[cluster] port=` when that port is non-zero; a listed self client port
must match the client listen port. Junk, duplicates, id `0`, or port `0`
fail startup. Empty `peers` is a single-node majority.
